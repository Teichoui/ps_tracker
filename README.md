# ps_tracker

DanNet-aware Power Source tracker and auto-swap for EverQuest boxing crews.

## Features

- ImGui overlay showing every toon's equipped PS: name, tier, power%, and status
- Auto-detects spent Power Sources and swaps in a fresh one from bags (same role)
- Handles sealed PS items — opens ItemDisplayWindow, clicks Convert, equips the result
- Coordinates trades across toons: the UI shows who has a spare and buttons to initiate
- Broadcasts PS state to all peers via DanNet actors (falls back to solo mode if actors unavailable)

## Requirements

- MacroQuest with Lua support
- MQ2DanNet (for multi-toon mode)
- MQ2Nav or MQ2MoveUtils (for trade navigation; falls back to /moveto if Nav is absent)

## Install

Copy the `ps_tracker` folder into your `MQ/lua/` directory so the layout is:

```
MQ/lua/ps_tracker/init.lua
```

## Usage

Run on every toon:

```
/lua run ps_tracker
```

### Commands

| Command | Description |
|---|---|
| `/ps stop` | Stop the script |
| `/ps ui` | Toggle the ImGui window |
| `/ps swap` | Manually trigger a PS swap now |
| `/ps scan` | Force immediate re-scan and broadcast |
| `/ps convert` | Right-click convert the first sealed PS in bags |

## Power Source database

The `PS_DB` table in `init.lua` lists every known PS item with its tier and role. Add new expansion items there as they release. Tiers go from 3 (old Ring of Scale orbs) up to the current expansion's tier.

Role mapping:

| Role | Classes |
|---|---|
| `tank` | WAR, PAL, SHD |
| `melee` | BER, ROG, MNK, RNG, BST, BRD |
| `caster` | WIZ, MAG, NEC, ENC |
| `healer` | CLR, DRU, SHM |

## License

MIT
