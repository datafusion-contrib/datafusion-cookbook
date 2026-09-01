# Contributing

Contributions of all kinds are welcome — new recipes, fixes to existing ones,
prompts, and reports of what worked or didn't when building from the
cookbook. 

Please feel free to open an issue or submit a pull request. 

Good places to start:

- Complete a recipe in [future-recipes/](future-recipes/).
- Run a prompt from [prompts/](prompts/) with your coding agent and
  report gaps, per the instructions in [TESTING.md](TESTING.md).

## Adding a new recipe or prompt

1. Copy [recipes/TEMPLATE.md](recipes/TEMPLATE.md) or
[prompts/TEMPLATE.md](prompts/TEMPLATE.md)
2. Fill in the template
3. Regenerate the index tables with `python3 scripts/regen_index.py`

Set `status: verified` only once the recipe's Verify step passes as written.
Incomplete recipes belong in [future-recipes/](future-recipes/) until they
verify.


## License

By contributing, you agree that your contributions will be licensed
under the [Apache License 2.0](LICENSE).
