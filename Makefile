# Citus toplevel Makefile

reindent:
	cd ${citus_abs_top_srcdir} && citus_indent --quiet
check-style:
	cd ${citus_abs_top_srcdir} && citus_indent --quiet --check
.PHONY: reindent check-style
