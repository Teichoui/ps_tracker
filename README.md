# ps_tracker

DanNet-aware Power Source tracker and auto-swap for EverQuest boxing crews.

## Features

- ImGui overlay showing every toon's equipped PS: name, tier, power%, and status
- Auto-detects spent or missing Power Sources and swaps in a fresh one from your own bags (same role). If no matching-role PS is available it equips the best other-role PS as a stopgap (shown as WRONG ROLE)
- Handles sealed PS items: opens ItemDisplayWindow, clicks Convert, equips the result (only if it fits your role and is an upgrade)
- Coordinates trades across toons: the UI shows who has a spare and a Trade button; trades are only ever requested by a click, never automatically
- Broadcasts PS state to all peers via the MacroQuest actors module
- Trades a single Power Source out of a stack (ctrl-click split), with a handshake between giver and recipient, a cursor check before every pickup, and a verify step afterwards
- Start All / Stop All / Restart All buttons run the script on every DanNet peer from one toon
- Never destroys or deletes items; see Safety notes

## Requirements

- MacroQuest with Lua support
- MQ2DanNet (only for the Start All / Stop All / Restart All buttons)
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
| `/ps swap` | Equip a fresh PS from your own bags now (works for a spent PS or an empty slot; does nothing if the equipped PS is not spent). The work runs on the next tick of the script |
| `/ps scan` | Force immediate re-scan and broadcast |
| `/ps convert` | Convert the first sealed PS in bags (via ItemDisplayWindow). The result stays unsealed in your bags. The command does not equip it, and automatic equipping is paused for about 10 seconds afterwards; a later automatic scan may equip it if your slot needs a PS |

## Safety notes

- **Start All / Stop All / Restart All** send the command to every DanNet peer, not just your group (never to the toon you click them on). Stop asks each peer to stop through its normal shutdown (`/ps stop`), so it cancels trades and puts items away; Restart only acts on peers that were reporting ps_tracker when you clicked, and starts each one again only after it is confirmed stopped: it said goodbye (the normal case), or, only for an older version that has never said goodbye and was heard just before the click, it sent no status at all for 15 seconds (3 broadcast intervals) counted from the moment the stop was sent; the rule used is logged, and the peer must still be silent right before the start. A toon is matched to its DanNet name by character and server; a DanNet name without a server, or two peers sharing a name, are skipped with a message. A peer not confirmed stopped within 25 seconds is skipped with a message (stop and start it once by hand), so a second copy is never started on a running one. A peer that is not running ps_tracker has no `/ps` command and cannot be stopped this way (use Start All for it).
- Incoming trades are only auto-accepted when the other party is a known peer that announced the trade first (the announcement must come from that peer's own script). The script cannot read what is actually in the trade window, so it trusts the announcement. Announcements are kept per giver, so two givers can announce at the same time and each window is matched to its own giver's announcement. An announcement is single-use, only authorizes a trade window that opens after it arrives (a window that was already open, or already ignored, is never authorized later, but the announcement is kept until it expires after 60 seconds), and is withdrawn if the giver gives up or the trade does not complete (each announcement carries a run id, so a withdrawal cannot clear a newer one). The recipient answers an announcement with an acknowledgement and the giver opens no trade window until it arrives (3 seconds, then the run is cancelled; older versions never acknowledge). The giver also re-checks that the recipient still needs the item right before placing it, and only places an item it picked up itself. After a closed window the item must stay gone for 2.5 seconds before the trade counts as complete; an aborted trade closes its own window and puts a returned item away. Actor messages are accepted only when the sender comes from this script's own mailbox (exactly ps_tracker, ps_tracker:ps_tracker or lua:ps_tracker:ps_tracker, as the game reports it), the sender's character and server are present, and the server is this toon's server; a sender without a server is ignored. Any other trade window is left alone, including one that was already open when a trade run starts. If the script is stopped while an auto-accepted incoming trade is still open with the same partner, that window is cancelled.
- The script never asks a peer for a Power Source on its own. If you need one that only a peer holds, it logs a hint; click the Trade button next to your own name in the overlay to request it. Equipping from your own bags (including converting your own sealed PS) is automatic. The Swap button (and a Swap request from another toon) only ever uses the bags of the toon being swapped; it never starts a trade.
- The script never destroys or deletes items. Before every pickup, equip or `/autoinventory` it reads the cursor; it only puts an item away when it is a known Power Source or the item it just picked up or swapped out, and otherwise leaves the cursor alone and aborts.
- A trade request is only honored from a known peer asking for itself (a peer cannot request a trade on behalf of someone else), only for a known Power Source in your own bags, and only if that peer actually needs one. Items flagged NO TRADE are never offered. A stack of more than one item is offered, and exactly ONE item is split off: the giver ctrl-clicks the stack (`/ctrl /itemnotify ...`, the same click used for single items); if a Quantity window opens it is set to 1 and accepted, with every step bounded and re-checked. Nothing is placed in the trade window unless the cursor then holds the expected item with a stack count of exactly 1; otherwise (wrong item, more than one, Quantity window still open or never opened, slider not reading 1) the run aborts, the Quantity window is closed, the cursor item is put back with `/autoinventory`, and no trade window is touched. The ctrl-click single-item pickup from a stack is confirmed in game; only the Quantity window branch (used if a quantity window opens instead) is still untested (set `STACK_TRADE_ENABLED = false` in init.lua to go back to skipping stacks). Converting a stacked sealed item is unchanged (it never picks the stack up). The giver announces each trade to the recipient, and the trade window is cancelled if the recipient does not accept within 20 seconds.
- Automatic equip and convert attempts are throttled; after repeated failures (including failed sealed converts) the automatic attempts pause until you run `/ps swap`.
- Nothing is picked up or equipped automatically while a trade, merchant, bank or loot window is open.
- Convert is irreversible, so it is only clicked when ItemDisplayWindow shows exactly the intended sealed item, and the result is accepted only when the count of the matching unsealed item (bags plus cursor) rose compared with just before the click, or the sealed source is gone; an unsealed item on the cursor without that evidence is left alone and never equipped. A confirmation dialog about attuning is only answered with Yes when it was not already open before the script's own click and does not name a different Power Source. Right before an equip click the powersource slot is re-checked; if it changed, the picked-up item is put away and the worn item is left alone.
- Peer names are only used in commands when they are letters only.
- Trade navigation uses exact-name spawn searches with a timeout and try limit; `/nav stop` is only sent for a navigation this script started. `/moveto` is only used when MQ2MoveUtils is loaded and MQ2Nav is not; it is issued once, and `/moveto off` is sent when the run arrives, aborts or the script stops.

## Tested status

Tested in game on Live: peer discovery, Start All, stack split by ctrl-click, a full give/accept trade, sealed convert with the attune prompt, and equip. Not yet tested in game: Stop All and Restart All, cancelled or timed-out trades, stopping the script mid-trade, the Quantity window branch, and whether the Cancel button click cancels a trade on the server. Reports are welcome.

The Trade button appears on your own row (to request a Power Source for yourself) and, on a spare-holder's row, only when you are the giver.

## Power Source database

Tier and role come from small tables in `init.lua`. Named sets ("Riven Arcana Slayer Source" and its "Sealed" form) are derived from `PS_SETS` (set name -> tier) and `PS_ROLE_WORDS` (Slayer, Protector, ...), so a new expansion set is normally one line in `PS_SETS`. One-off generic sources are listed by full name in `PS_DB`. Tiers go from 3 (old Ring of Scale orbs) up to the current expansion's tier. A power source the game recognizes (`Item.WornSlot('powersource')`) that is not in these tables is skipped, with a one-time chat message so it can be reported and added.

Role mapping:

| Role | Classes |
|---|---|
| `tank` | WAR, PAL, SHD |
| `melee` | BER, ROG, MNK, RNG, BST, BRD |
| `caster` | WIZ, MAG, NEC, ENC |
| `healer` | CLR, DRU, SHM |

## License

MIT
