"""
Tally Prime WhatsApp Bot - Powered by Twilio + Flask
"""

import os
import urllib.request
import xml.etree.ElementTree as ET
from flask import Flask, request
from twilio.twiml.messaging_response import MessagingResponse

app = Flask(__name__)

TALLY_HOST = os.environ.get("TALLY_HOST", "103.155.204.11")
TALLY_PORT = os.environ.get("TALLY_PORT", "45286")
TALLY_URL = f"http://{TALLY_HOST}:{TALLY_PORT}"

def send_tally_request(xml_request):
    data = xml_request.encode("utf-8")
    req = urllib.request.Request(TALLY_URL, data=data, headers={"Content-Type": "text/xml"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.read().decode("utf-8")
    except Exception:
        return None

def get_all_ledgers():
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
    if search:
        ledgers = [l for l in ledgers if search.lower() in l["name"].lower()]
        if not ledgers:
            return f"No ledger found matching '{search}'."
    msg = "*LEDGER REPORT*\n"
    msg += f"_Total: {len(ledgers)} ledgers_\n\n"
    for l in ledgers[:20]:
        bal = f"{l['balance']:,.2f}"
        msg += f"*{l['name']}*\n  Group: {l['group']}\n  Balance: Rs.{bal} {l['type']}\n\n"
    if len(ledgers) > 20:
        msg += f"_...and {len(ledgers) - 20} more. Use 'ledger <name>' to search._"
    return msg

def format_groups(groups):
    if not groups:
        return "Could not fetch groups."
    msg = "*LEDGER GROUPS*\n\n"
    for g in groups:
        parent = f" (under {g['parent']})" if g["parent"] else " (Primary)"
        msg += f"- {g['name']}{parent}\n"
    return msg

def format_daybook(vouchers):
    if not vouchers:
        return "No vouchers found or could not connect."
    msg = "*DAYBOOK*\n_Entries: " + str(len(vouchers)) + "_\n\n"
    for v in vouchers[:15]:
        try:
            amt = f"Rs.{abs(float(v['amount'])):,.2f}"
        except (ValueError, TypeError):
            amt = v["amount"]
        msg += f"*{v['type']}* - {v['party']}\n  Date: {v['date']} | Amount: {amt}\n\n"
    if len(vouchers) > 15:
        msg += f"_...and {len(vouchers) - 15} more entries._"
    return msg

def format_trial_balance(groups):
    if not groups:
        return "Could not generate trial balance."
    msg = "*TRIAL BALANCE*\n\n"
    total_dr, total_cr = 0, 0
    for g in sorted(groups.keys()):
        dr, cr = groups[g]["dr"], groups[g]["cr"]
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
    return "*TALLY WHATSAPP BOT*\n_M/S Aggarwal Agro Foods_\n\nAvailable commands:\n\n*ledgers* - All ledgers with balances\n*ledger <name>* - Search a specific ledger\n*groups* - All ledger groups\n*daybook* - Today's transactions\n*trial balance* - Trial balance summary\n*help* - Show this message\n\n_Just type a command and send!_"

@app.route("/webhook", methods=["POST"])
def webhook():
    incoming_msg = request.values.get("Body", "").strip().lower()
    resp = MessagingResponse()
    msg = resp.message()
    try:
        if incoming_msg in ("help", "hi", "hello"):
            msg.body(get_help_text())
        elif incoming_msg == "ledgers":
            ledgers = get_all_ledgers()
            msg.body(format_ledgers(ledgers) if ledgers else "Could not connect to Tally. Is Tally Prime running on CoCloud?")
        elif incoming_msg.startswith("ledger "):
            ledgers = get_all_ledgers()
            msg.body(format_ledgers(ledgers, search=incoming_msg[7:].strip()) if ledgers else "Could not connect to Tally.")
        elif incoming_msg == "groups":
            msg.body(format_groups(get_ledger_groups()))
        elif incoming_msg == "daybook":
            msg.body(format_daybook(get_daybook()))
        elif incoming_msg in ("trial balance", "tb"):
            msg.body(format_trial_balance(get_trial_balance()))
        else:
            msg.body(f"Unknown command: '{incoming_msg}'\n\nType *help* to see available commands.")
    except Exception as e:
        msg.body(f"Error: {str(e)}\n\nMake sure Tally Prime is running on CoCloud.")
    return str(resp)

@app.route("/health", methods=["GET"])
def health():
    return "Tally WhatsApp Bot is running!", 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
