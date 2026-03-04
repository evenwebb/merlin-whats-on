---
name: Merlin Whats On Production Notes
overview: Historical note for this repository. The original WTW/St Austell implementation plan is archived; current production behavior is documented in README.md.
todos: []
isProject: false
---

# Plan Status

This repository is now in Merlin Cinemas Cornwall production mode.

- The original WTW/St Austell plan is superseded.
- Current operational documentation is maintained in [README.md](README.md).

# Current Production Controls

- Multi-cinema scraping with fallback parser paths
- TMDb enrichment via GitHub secret only
- Change-detection fingerprint fast-path
- Health gates with excluded-cinema support
- Per-cinema consecutive-failure state and escalation
- GitHub Actions failure logs + issue dedupe by failure signature

# If Extending to Another Script

Use the README sections:

1. `Configuration`
2. `Health checks`
3. `Per-cinema failure handling`
4. `GitHub Actions`

