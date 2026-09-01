We evaluate the prompts and information in this repository in the following way:

## Goal: 

We aim
* Identify any improvements to this repository or upstream repositories that would make it easier and faster to build the systems described in this repo


## Metrics: 

Time to Awesome (TM): Due to Paul Dix (TODO find link): how quickly is the system 
builds and satisfies the example queries of the prompt.

Specifically
1. How fast (wallclock time) can the system create the first version of the system that satisfies the prompt?
2. How many tokens / other measure of effort does it take to get the system working and satisfying the prompt?
3. How much back and forth does the system require to get the system working and satisfying the prompt?
3. How well does the system satisfy the prompt? (e.g. does it support all the features of the prompt, or only some of them?)

## Methodology:

1. Remove any tool specific memory (e.g. `~/.claude/projects/software-datafusion-cookbook/memory/`) so past runs do not affect current run
1. Delete `workdir/` (it is gitignored) so leftover projects and generated data from past runs do not affect the current run
1. Use the specified coding tool and the prompt in the repository
2. Evaluate the system built based on the prompt
3. File any issues or pull requests to improve the system or the prompt, or the upstream repositories that are used to build the system.


You can use the following template to test / improve the repository:
```text
We are testing the recipes in this repository for how quickly a coding agent can
turn them into a working system

Please run the prompt below, complete the tool requested, and then list any
things that could be done to make the next run faster and more efficient. 
```

## Current Results

| Agent | Prompt | Speed to initial completion | Cost of initial completion | Required Back and Forth | Build Quality |
|-------|--------|-----------------------------|----------------------------|:-----------------------|---------------|
|       |        |                             |                            |                        |               |
|       |        |                             |                            |                        |               |
