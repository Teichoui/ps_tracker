# REDGUIDES

This script was developed with assistance from an AI coding assistant (Claude by Anthropic).

The author has read every line of code, tested it in-game across multiple toons and expansion
power source tiers, and understands its full behavior.  An LLM was used as a coding tool, not
as a replacement for understanding the code.

## What the AI helped with

- Structuring the non-blocking state machine (`equipSM`) for the convert-and-equip flow
- Diagnosing MQ null-window crashes in the `wininspect` diagnostic script
- Working out the correct MQ window child name (`IDW_ConvertButton`) for sealed PS conversion
- Ensuring ImGui callbacks contain no blocking `mq.delay()` calls

## What the author owns

- The full design: scan cadence, role mapping, tier priority, trade coordination flow
- All in-game testing: swap, convert, trade, attune dialog, cursor-item edge cases
- Ongoing maintenance as new expansions add PS tiers
