# Updating vendored dependencies

```bash
rm -rf safestringlib
git clone https://github.com/intel/safestringlib
rm -rf safestringlib/{.git,unittests}
git add safestringlib/
git commit -m "Update safestringlib"
git cherry-pick -x dc2a371d9f8b28ad0e68c5230bb998b4463d8131
```
