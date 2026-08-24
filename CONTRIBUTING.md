Contribution Agreement
======================

As a contributor, you represent that the code you submit is your
original work or that of your employer (in which case you represent you
have the right to bind your employer). By submitting code, you (and, if
applicable, your employer) are licensing the submitted code to
the open source community subject to the Apache 2.0 license. 

File Headers
=============

New files and files that are edited should include the following header:

```
/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
```

Existing files that contain the original LinkedIn/BSD header should retain that header (See [Charter, Section 7.a](CHARTER.md).)
For those files, the Apache header should be added below the existing LinkedIn/BSD header.

Responsible Disclosure of Security Vulnerabilities
==================================================

Please do not file reports on Github for security issues.
See [SECURITY.md](./SECURITY.md) for how to report a vulnerability.

Tips for Getting Your Pull Request (PR) Accepted
===========================================

1. Make sure all new features are tested and the tests pass -- i.e. a submitted PR should have already been tested for 
existing and new unit tests.
2. Bug fixes must include a test case demonstrating the error that it fixes.
3. Open an issue first and seek advice for your change before submitting a PR. Large features which have never been 
discussed are unlikely to be accepted.
4. Do not create a PR with "work-in-progress" (WIP) changes.
5. Use clear and concise titles for submitted PRs and issues.
6. Each PR should be linked to an existing issue corresponding to the PR 
(see [PR template](./.github/pull_request_template.md)), and PRs can be submitted directly when
repository's PR template is filled out with the details.
7. We strongly encourage the use of recommended code-style for the project 
(see [code-style.xml](./docs/code-style.xml)).
8. A pre-commit CheckStyle hook can be run by adding `./checkstyle/checkstyle-pre-commit` to your `.git/hooks/pre-commit` script.

Technical Steering Committee
=============================

The Technical Steering Committee (TSC) is responsible for technical oversight of the project.

TSC Chair: **Viktor Somogyi-Vass** - @viktorsomogyi
TSC Co-Chair: **Kyle Liberti** - @kyguy

### TSC members:

All TSC members are Maintainers and voting members.

  - **Adem Efe Gencer** - @efeg
  - **Hao Geng** - @CCisGG
  - **Nick Garvey** - @nickgarvey
  - **Maryan Hratson** - @mhratson
  - **Allen Wang** - @allenxwang
  - **Tamas Barnabas Egyed** - @egyedt / @egytom
  - **Chia-Ping Tsai** - @chia7712
  - **Krit Petty** - @bgrishinko
  - **Jiangjie (Becket) Qin** - @becketqin
  - **Viktor Somogyi-Vass** - @viktorsomogyi
  - **Omkhar Arasaratnam** - @omkhar
  - **Kondrat Bertalan** - @k0b3rIT
  - **Paolo Patierno** - @ppatierno
  - **Mickael Maison** - @mimaison
  - **Kyle Liberti** - @kyguy

### Emeritus members

TSC members who are no longer actively participating in project governance may move to Emeritus status.
Emeritus is an honorary recognition and is not a TSC role.
Emeritus members are not counted as part of the TSC for voting or quorum purposes.
A TSC member may voluntarily move to Emeritus status at any time.

The TSC will conduct a yearly review of member activity. 
Members who have not been actively participating will be contacted and asked about their continued involvement.
Members who wish to step back will be moved to Emeritus status.
An Emeritus member may return to active TSC membership through the same process as adding a new TSC member.

### Project Roles

- **Contributor**: Anyone who contributes code, documentation, or other technical artifacts
  to the project (See [Charter, Section 2.c.i](CHARTER.md)).

- **Maintainer**: A Contributor who has earned the ability to commit to the project's repository.
  A Contributor may become a Maintainer by a majority approval of the TSC.
  A Maintainer may be removed by a majority approval of the TSC (See [Charter, Section 2.c.ii-iii](CHARTER.md)).

- **TSC Member**: A Maintainer who participates in project governance and has voting rights.
  At this time, all TSC members are also Maintainers.
  A Maintainer may become a TSC member by a majority approval of the TSC (See Charter, Section 2.c.iii).
  Nominations may be made by any existing TSC member.

### Voting Process

The TSC aims to operate as a consensus-based community (See [Charter, Section 3.a](CHARTER.md)).
Most technical and day-to-day project decisions are made through lazy consensus.
Formal votes are reserved for when consensus cannot be reached or for governance changes.

#### Technical Decisions

Technical decisions include code changes, bug fixes, documentation updates, and technical proposals.

**PR Approval**: A pull request requires at least **2 maintainer approvals** to merge.
If the PR author is a maintainer, that counts as one approval.

**Lazy Consensus**: For larger technical decisions (e.g., design proposals, deprecations,
dependency changes), a proposal is announced on the mailing list or in a GitHub issue/PR.
If no maintainer raises a -1 within the voting period, the proposal is accepted.

  - Must remain open for at least **3 days** (72 hours)
  - Requires at least **3 binding +1 votes** and **zero binding -1 votes**
  - A -1 (objection) must include a technical justification; an objection without justification is not binding

Any maintainer may request that a lazy consensus decision be escalated to a formal vote by the TSC, which follows the rules specified in the Governance Decisions section below.

#### Governance Decisions

Governance decisions include changes to the CONTRIBUTING file, adding or removing Maintainers, and other non-technical project decisions.
These require a formal vote.

- **At a meeting**: A majority of those in attendance, provided quorum (50% of all voting TSC members) is met.
- **By electronic vote**: A majority of all voting members of the TSC.

Formal votes must remain open for at least **7 days** (168 hours).

Charter amendments require a two-thirds vote of the entire TSC and are subject
to approval by LF Projects (See [Charter, Section 8.a](CHARTER.md)).