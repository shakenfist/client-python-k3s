# Triage (step 4f)

Step 4f of `PLAN-node-customisation-phase-04-push-audit.md`: the
decisions on the four lenses' 42 findings, and the commits that acted
on them. Every finding was re-confirmed at the branch head before it
was acted on. None had moved: the branch head differed from `3e84907`
only in plan files when this step began.

Decision 7's exit rule was applied:
* `fix` is taken;
* `document` is taken when it is a comment, a docstring or a sentence
  of documentation;
* `consider` is taken when it is a one-liner or a real defect in code
  these phases added;
* `none` is informational.

## De-duplication

Five findings overlap across lenses. Each overlap is one row below.

* **T-6 and S-5** are the same YAML alias amplification. The tests
  lens rated it `none` and deferred it to the security lens, so it is
  triaged as S-5.
* **T-5's token-alias row** (`'token '`, `TOKEN`, `token_file`,
  `--token`) is the question S-1 answered from k3s's source. None of
  those spellings is an alias, but `t` and `token=abc` are. It is
  taken as S-1. The rest of T-5 has its own row.
* **D-6(c) and T-7** are the zero-worker taint exception's missing
  live coverage. T-7 says phase 3's decision still holds. D-6(c) says
  the decline should be findable from the master plan, which is 4g's
  Future work. D-7's sentence in `docs/testing.md` now also names it.
* **T-9 and T-1.** T-9 notes that a refused-key check would cost four
  lines if T-1 were taken. T-1 was taken without it, because the unit
  tier already runs that path through the real CLI group (T-9's own
  argument).
* **Rediscovered issues.**
  * The security lens's #104 note concerns `check_k3s_release()`,
    which is this plan's code, so it was fixed rather than commented
    on.
  * The tests lens's #93 note is a line phase 2 added, so it was fixed
    too.
  * #82 (CQ), #96 (CQ and T), #100 (CQ), #101 and #102 (T), #58 and
    #99 (D) needed nothing. #82's consequence was CQ-4, which is
    fixed. #96 already describes the sizing floors beside the
    unchecked counts. #100 is out of scope by decision 7. #58's grep is
    clean. #99 and #101/#102 were not touched.

  So no issue was commented on.

## Decisions

| Finding | Rating | Disposition | Why, commit, or issue |
|---|---|---|---|
| CQ-1 | document | taken | `d47c905` Correct stale comments in the k3s config code. |
| CQ-2 | document | taken | `d47c905` Correct stale comments in the k3s config code. |
| CQ-3 | document | taken | `d47c905` Correct stale comments in the k3s config code. |
| CQ-4 | consider | taken | `0cabb4b` Stop passing sort_keys to yaml.safe_dump(). It is a real defect, a PyYAML 5.1 floor, and the fix drops the keyword rather than pinning pyyaml. |
| CQ-5 | consider | declined | Filed as #111. It is a refactor that reworks two pre-existing call sites to tidy a third; all three are correct today. |
| CQ-6 | consider | declined | The lens called it optional. The regex is correct for what it claims, and `2b6cc8a` tightened it. A `Version` comparison would still need the tuple floor converted. |
| CQ-7 | document (advisory) | taken, in part | `d47c905`: `create()`'s near-verbatim copy of `docs/usage.md`'s measurement became a pointer. The others (`NodeSizeError`, `show()`, `_node_size()`) each carry their own contract. |
| CQ-8 | document (advisory) | declined | The block comment's list of the three files is the frame that its implementation-only reasons hang on. Cut to a pointer, those reasons would lose their referents. |
| CQ-9 | consider | declined | `K3sConfigError.unreadable()` shipped in v0.1.0 and v0.2.0, and classmethods are the documented construction interface. Renaming a keyword to delete one sentence is a small breaking change for nothing a caller sees. |
| CQ-10 | consider | declined | A refactor for its own sake: two three-way branches, each correct. |
| CQ-11 | consider | declined | A test-helper refactor for its own sake. |
| CQ-12 | consider | declined | A refactor of the functional script for its own sake. Each parser is correct, and the change would need a merge-tier run of its own. |
| CQ-13 | consider | declined | `docs/usage.md` is the name the docs use. An absolute URL in `--help` would pin a branch into the CLI contract snapshot for a two-word pointer. |
| CQ-14 | none | informational | The lens's answer to the duplication question. Nothing to do. |
| T-1 | consider | taken | `18db6d4` Check the release floor refusal in the merge tier. It cannot be verified locally, so the merge tier must be dispatched. |
| T-2 | consider | taken | `1bac64b` Make the bad-config CLI test see the bind order. |
| T-3 | consider | taken | `39766b6` Pin that a caller's own + keys are accepted. |
| T-4 | consider | taken | `9078f6b` Refuse infinite values in k3s configuration. |
| T-5 | consider | declined | The parts that were defects are taken elsewhere: T-3, T-4's infinities, S-1's aliases, and S-5's aliases and merge keys. The rest pins accepted shapes, none of which is a defect. |
| S-5, T-6 | consider | taken | `b98025f` Refuse YAML aliases in k3s configuration files. A mapping a library caller builds itself is unaffected, and a test pins that. |
| T-7, D-6(c) | none / document | 4g | T-7: phase 3's decision 2 stands. D-6(c): record the decline in the master plan's Future work. `docs/testing.md` now names it as unit-tested only (`18db6d4`). |
| T-8 | none | declined | Filed as #110. A pre-existing gap these phases widened slightly. |
| T-9 | none | informational | The unit tier runs the real CLI group and file read. |
| T-10 | none | informational | The `+` merge rule is already observed live through `disable+`. |
| T-11 | consider | taken | `3d7cd31` Add the k3s configuration properties to mutations. Ten entries; all 22 are caught. |
| T-12 | none | informational | Consistent with `ReleaseLookupError.unknown_channel`, and the docs' "before the name is registered" stays true. |
| D-1 | fix | taken | `0ef023c` Settle the OpenStack-Helm taint advice in usage. |
| D-2 | fix | taken | `0be7060` Correct the exception documentation's facts. |
| D-3 | document | taken | `0be7060` Correct the exception documentation's facts. This also corrects the `KubeconfigError` row that the lens left unverified. |
| D-4 | document | taken | `0ef023c` Settle the OpenStack-Helm taint advice in usage. |
| D-5 | consider | declined | The "Behaviour changes" list works as upgrade notes for someone moving from an older release, where pointers would serve worse. The three restated facts agree with their sources. |
| D-6(a) | document | 4g | The collection follow-on issue. Separately, `3246535` (Warn zero-worker collection clusters run pods.) adds the `docs/collection.md` note D-6(a) suggested, because `initial_workers` defaults to 0. |
| D-6(b) | document | 4g | The prototype-notes Future work bullet. |
| D-7 | consider | taken | `18db6d4` Check the release floor refusal in the merge tier. One sentence in `docs/testing.md`. |
| D-8 | consider | taken | `7541d25` Name the k3s config step in the assembly flow. One clause. |
| D-9 | document | 4g | Record which reading of the Success criteria' documentation clause was taken. |
| S-1 | consider | taken | `a6d91dd` Refuse k3s aliases of owned keys, and '=' in keys. The aliases (`d`, `o`, `s`, `t` on servers; `d`, `s`, `t` on agents) were checked against k3s's `server.go` and `agent.go` at v1.21.1+k3s1 and at `bdb2a3e`. |
| S-2 | consider | taken | `9ac4c47` Set the kubeconfig's server URL, not rewrite it. |
| S-3 | document | taken | `dd51c76` Say what caller k3s config exposes, and redact it. |
| S-4 | document | taken | `dd51c76` Say what caller k3s config exposes, and redact it. It adds the documentation and the `delete()` redaction. |
| S-6 | none | informational | Self-inflicted, with the user's own privileges. |
| S-7 | none | informational | Only the supplier's own stderr. |
| #104 (S, rediscovered) | consider | taken | `2b6cc8a` Read only well-formed k3s releases. Fixed rather than commented on #104, because `check_k3s_release()` is this plan's code. |
| #93 (T, rediscovered) | consider | taken | `1bac64b`: `encoding='utf-8'` on phase 2's tempfile. Fixed rather than commented on #93, because the line is this plan's. |

## Mutations

Each test added to guard a fix was checked by breaking the fix: the
file was copied, edited, the named tests run with the tox venv's
`stestr`, and the file restored from the copy. Every run below failed
the test it should, with the failure shown.

| Fix | Mutation | Result |
|---|---|---|
| T-2 | `k3s_create` binds the cluster context before reading the config files | Fails: `test_a_refused_key_exits_before_create_is_called`, "Expected 'create_namespace' to not have been called. Called 1 times." |
| T-3 | Every key ending in `+` except `tls-san+` is refused (the lens's M19, which survived all 355 tests before) | Fails: `test_a_callers_own_plus_key_is_accepted`, "key node-taint+ is set by shakenfist_client_k3s" |
| T-4 | `allow_nan=False` dropped from the representability check | Fails: `test_nan_and_infinities_are_refused`, which returned `'kubelet-arg:\n- .inf\n'`. NaN was still refused by the comparison; `.inf` was not. |
| S-1 | `'t'` removed from `K3S_AGENT_OWNED_KEYS` | Fails: `test_k3s_aliases_of_owned_keys_are_refused`, `test_every_agent_owned_key_is_refused_with_and_without_plus` (returned `'t: x\n'`), and `test_the_owned_key_sets_are_exactly_the_plans` |
| S-1 | The `=` check disabled | Fails: `test_a_key_containing_equals_is_refused`, which returned `'token=abc: x\n'` |
| S-5 | `read_k3s_config()` parses with plain `yaml.SafeLoader` | Fails: `test_an_alias_is_refused`, `test_a_merge_key_is_refused` and `test_nested_aliases_are_refused_before_they_are_expanded`, each returning the expanded mapping |
| S-2 | The old substitution of the floating address for `127.0.0.1` restored | Fails: `test_a_bind_address_is_replaced_too`, `'https://192.168.10.100:6443' != 'https://10.0.0.4:6443'`. `test_the_loopback_address_is_replaced` and `test_progress` still pass, which shows the normal case is unchanged. |
| S-4 | The `server_config` / `agent_config` clause dropped from `delete()`'s redaction | Fails: `test_delete_does_not_debug_log_the_callers_k3s_configuration`; the output contains `SECRET-S3-KEY` |
| #104 | The old prefix regex `^v(\d+)\.(\d+)\.(\d+)` restored | Fails: `test_a_component_must_be_a_few_ascii_digits` (full-width digits were accepted) and `test_a_release_which_only_starts_well_is_unparseable` (`'unparseable' != 'too_old'`) |

Then `python3 tools/mutation-check.py` ran with the ten new entries
from T-11, and reported "All 22 mutations were caught". Three of those
guard code that was already there: the delimiter check on
configuration text, and the two before-registration checks. They were
re-run with `-v` to confirm each failed the test named for it:
* `test_text_with_a_delimiter_line_is_refused`;
* `test_an_owned_server_key_registers_nothing`;
* `test_a_release_below_the_floor_registers_nothing`.

No test guards CQ-4. A PyYAML older than 5.1 is not installable in
this environment, and a test asserting a keyword's absence would test
the text rather than the behaviour.

## Counts

There are 44 rows.

| Disposition | Rows |
|---|---|
| Taken | 23 |
| Declined | 11 |
| 4g | 4 |
| Informational | 6 |

* **Taken (23):** CQ-1, CQ-2, CQ-3, CQ-4, CQ-7, T-1, T-2, T-3, T-4,
  S-5/T-6, T-11, D-1, D-2, D-3, D-4, D-7, D-8, S-1, S-2, S-3, S-4,
  #104, #93. These went into 16 commits before this file.
* **Declined (11):** CQ-5, CQ-6, CQ-8, CQ-9, CQ-10, CQ-11, CQ-12,
  CQ-13, T-5, T-8, D-5.
* **4g (4):** T-7/D-6(c), D-6(a), D-6(b), D-9.
* **Informational (6):** CQ-14, T-9, T-10, T-12, S-6, S-7.

23 + 11 + 4 + 6 = 44. The rows cover all 42 findings:
* 42 findings;
* plus two, because D-6 is split into (a), (b) and (c);
* less two merged pairs (S-5 with T-6, and T-7 with D-6(c));
* plus the two rediscovered issues fixed in code (#104, #93).

That is 42 + 2 − 2 + 2 = 44.

* **Issues filed:** 2. #110 is T-8, the HA control plane's missing live
  coverage. #111 is CQ-5, the caller-file read helper.
* **Issues commented on:** 0. #104's and #93's rediscoveries were
  fixed instead.
* **Mutations:** nine targeted runs, all caught, plus the script's 22,
  all caught.

`tools/ci_deploy_test.sh` changed in `18db6d4`. Before the pull request
opens, the merge tier must be dispatched on this branch, and its URL
recorded here or in the close-out.
