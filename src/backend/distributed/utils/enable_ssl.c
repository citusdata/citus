/*-------------------------------------------------------------------------
 *
 * enable_ssl.c
 *    UDF and Utilities for enabling ssl during citus setup
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */


#include "postgres.h"

/*
 * Make sure that functions marked as deprecated in OpenSSL 3.0 don't trigger
 * deprecation warnings by indicating that we're using the OpenSSL 1.0.1
 * compatibile API. Postgres does this by already in PG14, so we should not do
 * it otherwise we get warnings about redefining this value. This needs to be
 * done before including libpq.h.
 */
#include "miscadmin.h"

#include "libpq/libpq.h"
#include "nodes/parsenodes.h"
#include "postmaster/postmaster.h"
#include "utils/guc.h"

#include "pg_version_constants.h"

#include "distributed/connection_management.h"
#include "distributed/memutils.h"
#include "distributed/worker_protocol.h"

#ifdef USE_OPENSSL
#include "openssl/dsa.h"
#include "openssl/err.h"
#include "openssl/pem.h"
#include "openssl/rsa.h"
#include "openssl/ssl.h"
#include "openssl/x509.h"
#endif


#define ENABLE_SSL_QUERY "ALTER SYSTEM SET ssl TO on;"
#define RESET_CITUS_NODE_CONNINFO \
	"ALTER SYSTEM SET citus.node_conninfo TO 'sslmode=prefer';"

#define CITUS_AUTO_SSL_COMMON_NAME "citus-auto-ssl"
#define X509_SUBJECT_COMMON_NAME "CN"

#define POSTGRES_DEFAULT_SSL_CIPHERS "HIGH:MEDIUM:+3DES:!aNULL"

/*
 * Microsoft approved cipher string.
 * This cipher string implicitely enables only TLSv1.2+, because these ciphers
 * were all added in TLSv1.2. This can be confirmed by running:
 * openssl -v <below strings concatenated>
 */
#define CITUS_DEFAULT_SSL_CIPHERS "ECDHE-ECDSA-AES128-GCM-SHA256:" \
								  "ECDHE-ECDSA-AES256-GCM-SHA384:" \
								  "ECDHE-RSA-AES128-GCM-SHA256:" \
								  "ECDHE-RSA-AES256-GCM-SHA384:" \
								  "ECDHE-ECDSA-AES128-SHA256:" \
								  "ECDHE-ECDSA-AES256-SHA384:" \
								  "ECDHE-RSA-AES128-SHA256:" \
								  "ECDHE-RSA-AES256-SHA384"
#define SET_CITUS_SSL_CIPHERS_QUERY \
	"ALTER SYSTEM SET ssl_ciphers TO '" CITUS_DEFAULT_SSL_CIPHERS "';"


/* forward declaration of helper functions */
static void GloballyReloadConfig(void);


#ifdef USE_SSL

/* forward declaration of functions used when compiled with ssl */
static bool ShouldUseAutoSSL(void);
static bool CreateCertificatesWhenNeeded(void);
static EVP_PKEY * GeneratePrivateKey(void);
static X509 * CreateCertificate(EVP_PKEY *privateKey);
static bool StoreCertificate(EVP_PKEY *privateKey, X509 *certificate);
#endif /* USE_SSL */


PG_FUNCTION_INFO_V1(citus_setup_ssl);
PG_FUNCTION_INFO_V1(citus_check_defaults_for_sslmode);


/*
 * citus_setup_ssl is called during the first creation of a citus extension. It configures
 * postgres to use ssl if not already on. During this process it will create certificates
 * if they are not already installed in the configured location.
 */
Datum
citus_setup_ssl(PG_FUNCTION_ARGS)
{
#ifndef USE_SSL
	ereport(WARNING, (errmsg("can not setup ssl on postgres that is not compiled with "
							 "ssl support")));
#else /* USE_SSL */
	if (!EnableSSL && ShouldUseAutoSSL())
	{
		ereport(LOG, (errmsg("citus extension created on postgres without ssl enabled, "
							 "turning it on during creation of the extension")));

		/* execute the alter system statement to enable ssl on within postgres */
		Node *enableSSLParseTree = ParseTreeNode(ENABLE_SSL_QUERY);
		AlterSystemSetConfigFile((AlterSystemStmt *) enableSSLParseTree);

		if (strcmp(SSLCipherSuites, POSTGRES_DEFAULT_SSL_CIPHERS) == 0)
		{
			/*
			 * postgres default cipher suite is configured, these allow TSL 1 and TLS 1.1,
			 * citus will upgrade to TLS1.2+HIGH and above.
			 */
			Node *citusSSLCiphersParseTree = ParseTreeNode(SET_CITUS_SSL_CIPHERS_QUERY);
			AlterSystemSetConfigFile((AlterSystemStmt *) citusSSLCiphersParseTree);
		}

		/*
		 * ssl=on requires that a key and certificate are present, since we have
		 * enabled ssl mode here chances are the user didn't install credentials already.
		 *
		 * This function will check if they are available and if not it will generate a
		 * self singed certificate.
		 */
		CreateCertificatesWhenNeeded();

		GloballyReloadConfig();
	}
#endif /* USE_SSL */

	PG_RETURN_NULL();
}


/*
 * citus_check_defaults_for_sslmode is called in the extension upgrade path when
 * users upgrade from a previous version to a version that has ssl enabled by default, and
 * only when the changed default value conflicts with the setup of the user.
 *
 * Once it is determined that the default value for citus.node_conninfo is used verbatim
 * with ssl not enabled on the cluster it will reinstate the old default value for
 * citus.node_conninfo.
 *
 * In effect this is to not impose the overhead of ssl on an already existing cluster that
 * didn't have it enabled already.
 */
Datum
citus_check_defaults_for_sslmode(PG_FUNCTION_ARGS)
{
	bool configChanged = false;

	if (EnableSSL)
	{
		/* since ssl is on we do not have to change any sslmode back to prefer */
		PG_RETURN_NULL();
	}

	/*
	 * test if the node_conninfo setting is exactly set to the default value used when
	 * Citus started to enable SSL. This is to make sure upgrades restores the previous
	 * value so users will not have unexpected changes during upgrades.
	 */
	if (strcmp(NodeConninfo, "sslmode=require") == 0)
	{
		/* execute the alter system statement to reset node_conninfo to the old default */

		ereport(LOG, (errmsg("reset citus.node_conninfo to old default value as the new "
							 "value is incompatible with the current ssl setting")));

		Node *resetCitusNodeConnInfoParseTree = ParseTreeNode(RESET_CITUS_NODE_CONNINFO);
		AlterSystemSetConfigFile((AlterSystemStmt *) resetCitusNodeConnInfoParseTree);
		configChanged = true;
	}

	if (configChanged)
	{
		GloballyReloadConfig();
	}

	PG_RETURN_NULL();
}


/*
 * GloballyReloadConfig signals postmaster to reload the configuration as well as
 * reloading the configuration in the current backend. By reloading the configuration in
 * the current backend the changes will also be reflected in the current transaction.
 */
static void
GloballyReloadConfig()
{
	if (kill(PostmasterPid, SIGHUP))
	{
		ereport(WARNING, (errmsg("failed to send signal to postmaster: %m")));
	}
	ProcessConfigFile(PGC_SIGHUP);
}


#ifdef USE_SSL


/*
 * ShouldUseAutoSSL checks if citus should enable ssl based on the connection settings it
 * uses for outward connections. When the outward connection is configured to require ssl
 * it assumes the other nodes in the network have the same setting and therefore it will
 * automatically enable ssl during installation.
 */
static bool
ShouldUseAutoSSL(void)
{
	const char *sslmode = NULL;
	sslmode = GetConnParam("sslmode");

	if (sslmode != NULL && strcmp(sslmode, "require") == 0)
	{
		return true;
	}

	return false;
}


/*
 * CreateCertificatesWhenNeeded checks if the certificates exists. When they don't exist
 * they will be created. The return value tells whether or not new certificates have been
 * created. After this function it is guaranteed that certificates are in place. It is not
 * guaranteed they have the right permissions as we will not touch the keys if they exist.
 */
static bool
CreateCertificatesWhenNeeded()
{
	EVP_PKEY *privateKey = NULL;
	X509 *certificate = NULL;
	bool certificateWritten = false;
	SSL_CTX *sslContext = NULL;

	/*
	 * Since postgres might not have initialized ssl at this point we need to initialize
	 * it our self to be able to create a context. This code is less extensive then
	 * postgres' initialization but that will happen when postgres reloads its
	 * configuration with ssl enabled.
	 */
#ifdef HAVE_OPENSSL_INIT_SSL
	OPENSSL_init_ssl(OPENSSL_INIT_LOAD_CONFIG, NULL);
#else
	SSL_library_init();
#endif

	sslContext = SSL_CTX_new(SSLv23_method());
	if (!sslContext)
	{
		ereport(WARNING, (errmsg("unable to create ssl context, please verify ssl "
								 "settings for postgres"),
						  errdetail("Citus could not create the ssl context to verify "
									"the ssl settings for postgres and possibly setup "
									"certificates. Since Citus requires connections "
									"between nodes to use ssl communication between "
									"nodes might return an error until ssl is setup "
									"correctly.")));
		return false;
	}
	EnsureReleaseResource((MemoryContextCallbackFunction) (&SSL_CTX_free),
						  sslContext);

	/*
	 * check if we can load the certificate, when we can we assume the certificates are in
	 * place. No need to create the certificates and we can exit the function.
	 *
	 * This also makes the whole ssl enabling idempotent as writing the certificate is the
	 * last step.
	 */
	if (SSL_CTX_use_certificate_chain_file(sslContext, ssl_cert_file) == 1)
	{
		return false;
	}
	ereport(LOG, (errmsg("no certificate present, generating self signed certificate")));

	privateKey = GeneratePrivateKey();
	if (!privateKey)
	{
		ereport(ERROR, (errmsg("error while generating private key")));
	}

	certificate = CreateCertificate(privateKey);
	if (!certificate)
	{
		ereport(ERROR, (errmsg("error while generating certificate")));
	}

	certificateWritten = StoreCertificate(privateKey, certificate);
	if (!certificateWritten)
	{
		ereport(ERROR, (errmsg("error while storing key and certificate")));
	}

	return true;
}


/*
 * GeneratePrivateKey uses open ssl functions to generate an RSA private key of 2048 bits.
 * All OpenSSL resources created during the process are added to the memory context active
 * when the function is called and therefore should not be freed by the caller.
 */
static EVP_PKEY *
GeneratePrivateKey()
{
	/* Allocate memory for the EVP_PKEY structure. */
	EVP_PKEY *privateKey = EVP_PKEY_new();
	if (!privateKey)
	{
		ereport(ERROR, (errmsg("unable to allocate space for private key")));
	}
	EnsureReleaseResource((MemoryContextCallbackFunction) (&EVP_PKEY_free),
						  privateKey);

	BIGNUM *exponent = BN_new();
	EnsureReleaseResource((MemoryContextCallbackFunction) (&BN_free), exponent);

	/* load the exponent to use for the generation of the key */
	int success = BN_set_word(exponent, RSA_F4);
	if (success != 1)
	{
		ereport(ERROR, (errmsg("unable to prepare exponent for RSA algorithm")));
	}

	RSA *rsa = RSA_new();
	success = RSA_generate_key_ex(rsa, 2048, exponent, NULL);
	if (success != 1)
	{
		ereport(ERROR, (errmsg("unable to generate RSA key")));
	}

	if (!EVP_PKEY_assign_RSA(privateKey, rsa))
	{
		ereport(ERROR, (errmsg("unable to assign RSA key to use as private key")));
	}

	/* The key has been generated, return it. */
	return privateKey;
}


/*
 * CreateCertificate creates a self signed certificate for citus to use. The certificate
 * will contain the public parts of the private key and will be signed in the end by the
 * private part to make it self signed.
 */
static X509 *
CreateCertificate(EVP_PKEY *privateKey)
{
	X509 *certificate = X509_new();
	if (!certificate)
	{
		ereport(ERROR, (errmsg("unable to allocate space for the x509 certificate")));
	}
	EnsureReleaseResource((MemoryContextCallbackFunction) (&X509_free),
						  certificate);

	/* Set the serial number. */
	ASN1_INTEGER_set(X509_get_serialNumber(certificate), 1);

	/*
	 * Set the expiry of the certificate.
	 *
	 * the functions X509_get_notBefore and X509_get_notAfter are deprecated, these are
	 * replaced with mutable and non-mutable variants in openssl 1.1, however they are
	 * better supported than the newer versions. In 1.1 they are aliasses to the mutable
	 * variant (X509_getm_notBefore, ...) that we actually need, so they will actually use
	 * the correct function in newer versions.
	 *
	 * Postgres does not check the validity on the certificates, but we can't omit the
	 * dates either to create a certificate that can be parsed. We settled on a validity
	 * of 0 seconds. When postgres would fix the validity check in a future version it
	 * would fail right after an upgrade. Instead of working until the certificate
	 * expiration date and then suddenly erroring out.
	 */
	X509_gmtime_adj(X509_get_notBefore(certificate), 0);
	X509_gmtime_adj(X509_get_notAfter(certificate), 0);

	/* Set the public key for our certificate */
	X509_set_pubkey(certificate, privateKey);

	/* Set the common name for the certificate */
	X509_NAME *subjectName = X509_get_subject_name(certificate);
	X509_NAME_add_entry_by_txt(subjectName, X509_SUBJECT_COMMON_NAME, MBSTRING_ASC,
							   (unsigned char *) CITUS_AUTO_SSL_COMMON_NAME, -1, -1,
							   0);

	/* For a self signed certificate we set the isser name to our own name */
	X509_set_issuer_name(certificate, subjectName);

	/* With all information filled out we sign the certificate with our own key */
	if (!X509_sign(certificate, privateKey, EVP_sha256()))
	{
		ereport(ERROR, (errmsg("unable to create signature for the x509 certificate")));
	}

	return certificate;
}


/*
 * StoreCertificate stores both the private key and its certificate to the files
 * configured in postgres.
 */
static bool
StoreCertificate(EVP_PKEY *privateKey, X509 *certificate)
{
	const char *privateKeyFilename = ssl_key_file;
	const char *certificateFilename = ssl_cert_file;


	/* Open the private key file and write the private key in PEM format to it */
	int privateKeyFileDescriptor = open(privateKeyFilename, O_WRONLY | O_CREAT, 0600);
	if (privateKeyFileDescriptor == -1)
	{
		ereport(ERROR, (errmsg("unable to open private key file '%s' for writing",
							   privateKeyFilename)));
	}

	FILE *privateKeyFile = fdopen(privateKeyFileDescriptor, "wb");
	if (!privateKeyFile)
	{
		ereport(ERROR, (errmsg("unable to open private key file '%s' for writing",
							   privateKeyFilename)));
	}

	int success = PEM_write_PrivateKey(privateKeyFile, privateKey, NULL, NULL, 0, NULL,
									   NULL);
	fclose(privateKeyFile);
	if (!success)
	{
		ereport(ERROR, (errmsg("unable to store private key")));
	}

	int certificateFileDescriptor = open(certificateFilename, O_WRONLY | O_CREAT, 0600);
	if (certificateFileDescriptor == -1)
	{
		ereport(ERROR, (errmsg("unable to open private key file '%s' for writing",
							   privateKeyFilename)));
	}

	/* Open the certificate file and write the certificate in the PEM format to it */
	FILE *certificateFile = fdopen(certificateFileDescriptor, "wb");
	if (!certificateFile)
	{
		ereport(ERROR, (errmsg("unable to open certificate file '%s' for writing",
							   certificateFilename)));
	}

	success = PEM_write_X509(certificateFile, certificate);
	fclose(certificateFile);
	if (!success)
	{
		ereport(ERROR, (errmsg("unable to store certificate")));
	}

	return true;
}


#endif /* USE_SSL */
