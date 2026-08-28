---
name: rustyline
title: REPL Line Editing
category: repl
summary: Readline-style input for a CLI — history, emacs/vi keybindings, multi-line editing, completion hooks.
when_to_use: You are building an interactive prompt and want arrow-key history and line editing rather than bare `stdin.read_line`.
crate: rustyline
version: "18.0.1"
datafusion: any
license: MIT
repo: https://github.com/kkawakam/rustyline
install: cargo add rustyline@18.0.1
status: stable
pitfalls:
  - "MIT licensed, not Apache-2.0. Both are permissive and GPL-compatible, but if a prompt specifies Apache-2.0 exclusively, call this out rather than silently adding it."
  - "SQL input is multi-line and terminated by `;`. `readline()` returns a single line, so you must accumulate lines yourself until you see the semicolon."
example: https://github.com/kkawakam/rustyline/tree/master/examples
---

The minimum useful SQL REPL loop — note the accumulation until `;`, which is
the part most implementations get wrong on the first pass:

```rust
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;

let mut rl = DefaultEditor::new()?;
let mut buffer = String::new();

loop {
    let prompt = if buffer.is_empty() { "yadb> " } else { "  ...> " };
    match rl.readline(prompt) {
        Ok(line) => {
            buffer.push_str(&line);
            buffer.push(' ');

            if buffer.trim_end().ends_with(';') {
                let sql = buffer.trim().trim_end_matches(';').to_string();
                rl.add_history_entry(&sql)?;
                // run `sql` against the SessionContext here
                buffer.clear();
            }
        }
        Err(ReadlineError::Interrupted) => { buffer.clear(); }  // Ctrl-C clears the line
        Err(ReadlineError::Eof) => break,                        // Ctrl-D exits
        Err(e) => return Err(e.into()),
    }
}
```

Handle `Interrupted` and `Eof` distinctly: Ctrl-C should abandon the
half-typed statement, Ctrl-D should exit. Collapsing them into one arm is a
common source of "the CLI ignores Ctrl-C" feedback.
