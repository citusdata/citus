# Updating vendored dependencies

```bash
rm -rf safestringlib
git clone https://github.com/intel/safestringlib
rm -rf safestringlib/{.git,unittests}
git add safestringlib/
git commit -m "Update safestringlib"
git cherry-pick -x 92d7a40d1de472d23d05967be9b77eda30af85cb
```
