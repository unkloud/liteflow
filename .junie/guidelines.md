## General Guidelines:

* **No system component installations**.
* **DONT RUN ANY COMMAND WITH sudo**.
* For each change prompted
    * Come up with a execution plan, generate a check list for each steps
    * Prompt for conformation and adjustment
    * After confirmed, execute the plan step by step, follow the instructions as close as possible
    * Keep code compact but maintain readability
        * Avoid excessive blank lines while respecting language idioms (e.g. PEP 8)
        * Avoid printing out messages as separation of functional blocks, use functions instead
    * Limit comments to complex logics only. Break complex logic into smaller explainable components instead.
        * Add simple comments to explain the intent of the complex logic.
        * Add simple example code to show how to use complex functions
    * Add simple unit tests to test complex functions
* Test cases to cover essential functionalities; exhaustive test coverage isn’t necessary
* Failing early is preferable, also provide clear error messages
* Append to `.agent/changelog.md`, summarize changes in one sentence
* Document AI coding agent contribution in the `.agent/changelog.md`

## Python Scripts:

* Follow Python best practices; prioritize clean, readable, idiomatic code
* Minimize print statements.
* Format and lint with `black` and `ruff` on every save.

## Bash Scripts:

* Use simple, idiomatic, and clean Bash practices.
* Avoid clever tricks; keep scripts readable.
* Validate scripts with `shellcheck`.

## D Projects:

* **Compiler Setup:**
  Activate D compilers with:
  ```bash
  source $HOME/dlang/dmd-2.111.0/activate
  source $HOME/dlang/gdc-4.8.5/activate
  source $HOME/dlang/ldc-1.41.0/activate
  ```
  Use `deactivate` to turn off the compiler. Fail with an error if no compiler is available, do not try to install
  compilers
* Write straightforward, self-explanatory code; avoid meta-programming unless necessary.
* Prefer clarity over premature optimizations. Design modules for performance and modularity.
* Use reliable dependencies (`vibe-d` is highly recommended).
* Avoid deep class hierarchies. Opt for functional composition where possible.
* Run `dfmt` on every change.
* **Code Style**:
    * Avoid abstractions unless absolutely required.
    * Minimize logging, print statements, and empty lines. Compact code is preferred.

# Golang project

* Follow best practices in Go.
* Prefer standard library over 3rd party libraries
* Don't use 3rd party libraries unless absolutely required
    * If some functionality is not available in standard library
        * If the functionality has clearly understood implementation, just create a new package and implement it
    * Prefer 3rd party libraries contributed by companies like:
        * Google, Paypal, Meta, Microsoft, Alibaba, Huawei, Cloudflare, Docker, etc.
        * Very reputable OSS communities
