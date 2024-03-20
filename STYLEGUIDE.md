# Coding style

The existing code-style in our code-base is not super consistent. There are multiple reasons for that. One big reason is because our code-base is relatively old and our standards have changed over time. The second big reason is that our style-guide is different from style-guide of Postgres and some code is copied from Postgres source code and is slightly modified. The below rules are for new code. If you're changing existing code that uses a different style, use your best judgement to decide if you use the rules here or if you match the existing style.

## Using citus_indent

CI pipeline will automatically reject any PRs which do not follow our coding
conventions. The easiest way to ensure your PR adheres to those conventions is
to use the [citus_indent](https://github.com/citusdata/tools/tree/develop/uncrustify)
tool. This tool uses `uncrustify` under the hood.

```bash
# Uncrustify changes the way it formats code every release a bit. To make sure
# everyone formats consistently we use version 0.68.1:
curl -L https://github.com/uncrustify/uncrustify/archive/uncrustify-0.68.1.tar.gz | tar xz
cd uncrustify-uncrustify-0.68.1/
mkdir build
cd build
cmake ..
make -j5
sudo make install
cd ../..

git clone https://github.com/citusdata/tools.git
cd tools
make uncrustify/.install
```

Once you've done that, you can run the `make reindent` command from the top
directory to recursively check and correct the style of any source files in the
current directory. Under the hood, `make reindent` will run `citus_indent` and
some other style corrections for you.

You can also run the following in the directory of this repository to
automatically format all the files that you have changed before committing:

```bash
cat > .git/hooks/pre-commit << __EOF__
#!/bin/bash
citus_indent --check --diff || { citus_indent --diff; exit 1; }
__EOF__
chmod +x .git/hooks/pre-commit
```

## Other rules we follow that citus_indent does not enforce

* We almost always use **CamelCase**, when naming functions, variables etc., **not snake_case**.

* We also have the habits of using a **lowerCamelCase** for some variables named from their type or from their function name, as shown in the examples:

  ```c
  bool IsCitusExtensionLoaded = false;


  bool
  IsAlterTableRenameStmt(RenameStmt *renameStmt)
  {
    AlterTableCmd *alterTableCommand = NULL;
    ..
    ..

    bool isAlterTableRenameStmt = false;
    ..
  }
  ```

* We **start functions with a comment**:

  ```c
  /*
   * MyNiceFunction <something in present simple tense, e.g., processes / returns / checks / takes X as input / does Y> ..
   * <some more nice words> ..
   * <some more nice words> ..
   */
  <static?> <return type>
  MyNiceFunction(..)
  {
    ..
    ..
  }
  ```

* `#includes` needs to be sorted based on below ordering and then alphabetically and we should not include what we don't need in a file:

  * System includes (eg. #include<...>)
  * Postgres.h (eg. #include "postgres.h")
  * Toplevel imports from postgres, not contained in a directory (eg. #include "miscadmin.h")
  * General postgres includes (eg . #include "nodes/...")
  * Toplevel citus includes, not contained in a directory (eg. #include "citus_verion.h")
  * Columnar includes (eg. #include "columnar/...")
  * Distributed includes (eg. #include "distributed/...")

* Comments:
  ```c
  /* single line comments start with a lower-case */

  /*
   * We start multi-line comments with a capital letter
   * and keep adding a star to the beginning of each line
   * until we close the comment with a star and a slash.
   */
  ```

* Order of function implementations and their declarations in a file:

  We define static functions after the functions that call them. For example:

  ```c
  #include<..>
  #include<..>
  ..
  ..
  typedef struct
  {
    ..
    ..
  } MyNiceStruct;
  ..
  ..
  PG_FUNCTION_INFO_V1(my_nice_udf1);
  PG_FUNCTION_INFO_V1(my_nice_udf2);
  ..
  ..
  // ..  somewhere on top of the file …
  static void MyNiceStaticlyDeclaredFunction1(…);
  static void MyNiceStaticlyDeclaredFunction2(…);
  ..
  ..


  void
  MyNiceFunctionExternedViaHeaderFile(..)
  {
    ..
    ..
    MyNiceStaticlyDeclaredFunction1(..);
    ..
    ..
    MyNiceStaticlyDeclaredFunction2(..);
    ..
  }

  ..
  ..

  // we define this first because it's called by MyNiceFunctionExternedViaHeaderFile()
  // before MyNiceStaticlyDeclaredFunction2()
  static void
  MyNiceStaticlyDeclaredFunction1(…)
  {
  }
  ..
  ..

  // then we define this
  static void
  MyNiceStaticlyDeclaredFunction2(…)
  {
  }
  ```
