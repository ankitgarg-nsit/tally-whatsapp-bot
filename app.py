"""
Tally Prime WhatsApp Bot - Powered by Twilio + Flask
Receives WhatsApp messages, queries Tally on CoCloud, returns formatted results.

Commands:
  ledgers           - List all ledgers with closing balances
  ledger <name>     - Search for a specific ledger
  groups            - List all ledger groups
  daybook           - Today's transactions
  cashbook          - Cash/Bank transactions
  trial balance     - Trial balance summary
  help              - Show available commands
"""

import os
import re
import urllib.request
import xml.etree.ElementTree as ET
from flask import Flask, request
from twilio.twiml.messaging_response import MessagingResponse

app = Flask(__name__)

TALLY_HOST = os.environ.get("TALLY_HOST", "103.155.204.11")
TALLY_PORT = os.environ.get("TALLY_PORT", "45286")
TALLY_URL = f"http://{TALLY_HOST}:{TALLY_PORT}"

def sanitize_xml(xml_str):
    """Remove invalid XML characters that Tally sometimes returns."""
    xml_str = re.sub(r'&#x([0-8bcefBCEF]|1[0-9a-fA-F]);', '', xml_str)
    xml_str = re.sub(r'&#([0-8]|1[0-1]|1[4-9]|2[0-9]|3[0-1]);', '', xml_str)
    xml_str = re.sub('[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', xml_str)
    return xml_str

def send_tally_request(xml_request):
    """Send XML request to Tally and return response string."""
    data = xml_request.encode("utf-8")
    req = urllib.request.Request(TALLY_URL, data=data, headers={"Content-Type": "text/xml"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            return sanitize_xml(raw)
    except Exception:
        return None

def get_all_ledgers():
    """Fetch all ledgers with parent group and closing balance."""
    xml = """<ENVELOPE><HEADER><VERSION>1</VERSION><TALLYREQUEST>Export</TALLYREQUEST><TYPE>Collection</TYPE><ID>AllLedgers</ID></HEADER><BODY><DESC><STATICVARIABLES><SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT></STATICVARIABLES><TDL><TDLMESSAGE><COLLECTION NAME="AllLedgers" ISMODIFY="No"><TYPE>Ledger</TYPE><FETCH>NAME, PARENT, CLOSINGBALANCE</FETCH></COLLECTION></TDLMESSAGE></TDL></DESC></BODY></ENVELOPE>"""
    response = send_tally_request(xml)
    if not response:
        return None
    ledgers = []
    root = ET.fromstring(response)
    for ledger in root.iter("LEDGER"):
        name = ledger.get("NAME", "")
        parent = ledger.find("PARENT")
        closing = ledger.find("CLOSINGBALANCE")
        parent_text = parent.text if parent is not None and parent.text else ""
        try:
            closing_val = float(closing.text.replace(",", "").strip()) if closing is not None and closing.text else 0.0
        except ValueError:
            closing_val = 0.0
        bal_type = "Dr" if closing_val >= 0 else "Cr"
        ledgers.append({"name": name, "group": parent_text, "balance": abs(closing_val), "type": bal_type})
    return ledgers

def get_ledger_groups():
    """Fetch all ledger groups."""
    xml = """<ENVELOPE><HEADER><VERSION>1</VERSION><TALLYREQUEST>Export</TALLYREQUEST><TYPE>Collection</TYPE><ID>AllGroups</ID></HEADER><BODY><DESC><STATICVARIABLES><SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT></STATICVARIABLES><TDL><TDLMESSAGE><COLLECTION NAME="AllGroups" ISMODIFY="No"><TYPE>Group</TYPE><FETCH>NAME, PARENT</FETCH></COLLECTION></TDLMESSAGE></TDL></DESC></BODY></ENVELOPE>"""
    response = send_tally_request(xml)
    if not response:
        return None
    groups = []
    root = ET.fromstring(response)
    for group in root.iter("GROUP"):
        name = group.get("NAME", "")
        parent = group.find("PARENT")
        parent_text = parent.text if parent is not None and parent.text else ""
        groups.append({"name": name, "parent": parent_text})
    return groups

def get_daybook():
    """Fetch today's voucher entries."""
    xml = """<ENVELOPE><HEADER><VERSION>1</VERSION><TALLYREQUEST>Export</TALLYREQUEST><TYPE>Collection</TYPE><ID>DayBookVouchers</ID></HEADER><BODY><DESC><STATICVARIABLES><SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT><SVCURRENTCOMPANY/></STATICVARIABLES><TDL><TDLMESSAGE><COLLECTION NAME="DayBookVouchers" ISMODIFY="No"><TYPE>Voucher</TYPE><FETCH>DATE, VOUCHERTYPENAME, PARTYLEDGERNAME, AMOUNT</FETCH></COLLECTION></TDLMESSAGE></TDL></DESC></BODY></ENVELOPE>"""
    response = send_tally_request(xml)
    if not response:
        return None
    vouchers = []
    root = ET.fromstring(response)
    for v in root.iter("VOUCHER"):
        date = v.find("DATE")
        vtype = v.find("VOUCHERTYPENAME")
        party = v.find("PARTYLEDGERNAME")
        amount = v.find("AMOUNT")
        vouchers.append({"date": date.text if date is not None and date.text else "", "type": vtype.text if vtype is not None and vtype.text else "", "party": party.text if party is not None and party.text else "", "amount": amount.text if amount is not None and amount.text else "0"})
    return vouchers

def get_trial_balance():
    """Fetch trial balance (group-wise closing balances)."""
    ledgers = get_all_ledgers()
    if not ledgers:
        return None
    groups = {}
    for l in ledgers:
        g = l["group"] or "Ungrouped"
        if g not in groups:
            groups[g] = {"dr": 0, "cr": 0}
        if l["type"] == "Dr":
            groups[g]["dr"] += l["balance"]
        else:
            groups[g]["cr"] += l["balance"]
    return groups

def format_ledgers(ledgers, search=None):
    """Format ledger list as WhatsApp-friendly text."""
    if search:
        ledgers = [l for l in ledgers if search.lower() in l["name"].lower()]
        if not ledgers:
            return f"No ledger found matching '{search}'."
    msg = "*LEDGER REPORT*\n"
    msg += f"_Total: {len(ledgers)} ledgers_\n\n"
    display = ledgers[:20]
    for l in display:
        bal = f"{l['balance']:,.2f}"
        msg += f"*{l['name']}*\n"
        msg += f"  Group: {l['group']}\n"
        msg += f"  Balance: Rs.{bal} {l['type']}\n\n"
    if len(ledgers) > 20:
        msg += f"_...and {len(ledgers) - 20} more. Use 'ledger <name>' to search._"
    return msg

def format_groups(groups):
    """Format group list."""
    if not groups:
        return "Could not fetch groups."
    msg = "*LEDGER GROUPS*\n\n"
    for g in groups:
        parent = f" (under {g['parent']})" if g["parent"] else " (Primary)"
        msg += f"- {g['name']}{parent}\n"
    return msg

def format_daybook(vouchers):
    """Format daybook entries."""
    if not vouchers:
        return "No vouchers found or could not connect."
    msg = "*DAYBOOK*\n"
    msg += f"_Entries: {len(vouchers)}_\n\n"
    for v in vouchers[:15]:
        try:
            amt = f"Rs.{abs(float(v['amount'])):,.2f}"
        except (ValueError, TypeError):
            amt = v["amount"]
        msg += f"*{v['type']}* - {v['party']}\n"
        msg += f"  Date: {v['date']} | Amount: {amt}\n\n"
    if len(vouchers) > 15:
        msg += f"_...and {len(vouchers) - 15} more entries._"
    return msg

def format_trial_balance(groups):
    """Format trial balance."""
    if not groups:
        return "Could not generate trial balance."
    msg = "*TRIAL BALANCE*\n\n"
    total_dr, total_cr = 0, 0
    for g in sorted(groups.keys()):
        dr = groups[g]["dr"]
        cr = groups[g]["cr"]
        total_dr += dr
        total_cr += cr
        if dr > 0 or cr > 0:
            msg += f"*{g}*\n"
            if dr > 0:
                msg += f"  Dr: Rs.{dr:,.2f}\n"
            if cr > 0:
                msg += f"  Cr: Rs.{cr:,.2f}\n"
            msg += "\n"
    msg += f"---\n*TOTAL Dr: Rs.{total_dr:,.2f}*\n*TOTAL Cr: Rs.{total_cr:,.2f}*"
    return msg

def get_help_text():
    """Return help message."""
    return ("*TALLY WHATSAPP BOT*\n" "_M/S Aggarwal Agro Foods_\n\n" "Available commands:\n\n" "*ledgers* - All ledgers with balances\n" "*ledger <name>* - Search a specific ledger\n" "*groups* - All ledger groups\n" "*daybook* - Today's transactions\n" "*trial balance* - Trial balance summary\n" "*help* - Show this message\n\n" "_Just type a command and send!_")

@app.route("/webhook", methods=["POST"])
def webhook():
    """Handle incoming WhatsApp messages from Twilio."""
    incoming_msg = request.values.get("Body", "").strip().lower()
    sender = request.values.get("From", "")
    resp = MessagingResponse()
    msg = resp.message()
    try:
        if incoming_msg == "help" or incoming_msg == "hi" or incoming_msg == "hello":
            msg.body(get_help_text())
        elif incoming_msg == "ledgers":
            ledgers = get_all_ledgers()
            if ledgers:
                msg.body(format_ledgers(ledgers))
            else:
                msg.body("Could not connect to Tally. Is Tally Prime running on CoCloud?")
        elif incoming_msg.startswith("ledger "):
            search_term = incoming_msg[7:].strip()
            ledgers = get_all_ledgers()
            if ledgers:
                msg.body(format_ledgers(ledgers, search=search_term))
            else:
                msg.body("Could not connect to Tally.")
        elif incoming_msg == "groups":
            groups = get_ledger_groups()
            msg.body(format_groups(groups))
        elif incoming_msg == "daybook":
            vouchers = get_daybook()
            msg.body(format_daybook(vouchers))
        elif incoming_msg in ("trial balance", "tb"):
            groups = get_trial_balance()
            msg.body(format_trial_balance(groups))
        else:
            msg.body(f"Unknown command: '{incoming_msg}'\n\nType *help* to see available commands.")
    except Exception as e:
        msg.body(f"Error: {str(e)}\n\nMake sure Tally Prime is running on CoCloud.")
    return str(resp)

@app.route("/health", methods=["GET"])
def health():
    """Health check endpoint for Render."""
    return "Tally WhatsApp Bot is running!", 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
