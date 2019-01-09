install: all installdirs
	$(INSTALL_PROGRAM) isolationtester$(X) '$(DESTDIR)$(pgxsdir)/$(subdir)/isolationtester$(X)'
	$(INSTALL_PROGRAM) pg_isolation_regress$(X) '$(DESTDIR)$(pgxsdir)/$(subdir)/pg_isolation_regress$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(pgxsdir)/$(subdir)'

uninstall:
	rm -f '$(DESTDIR)$(pgxsdir)/$(subdir)/isolationtester$(X)'
	rm -f '$(DESTDIR)$(pgxsdir)/$(subdir)/pg_isolation_regress$(X)'
