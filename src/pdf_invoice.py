#!/usr/bin/env python3
"""
Branded PDF invoice generator (WeasyPrint) + detailed XLS.

Produces BOTH outputs for a client over an epoch range:
  * PDF  - the payable, branded invoice (page 1) + a metrics page (page 2),
           rendered from templates/invoice.html with WeasyPrint.
  * XLS  - the existing detailed workbook (generate_invoice.InvoiceGenerator),
           filtered to the client's nodes, kept for line-level detail.

Both consume the same unified model from ``invoice_data.build_invoice`` so the
billed totals on the PDF and the detail in the XLS stay consistent.
"""

import argparse
import base64
import logging
import os
from pathlib import Path
from typing import Optional

import requests
from jinja2 import Environment, FileSystemLoader, select_autoescape

from invoice_data import build_invoice
from clients import get_client
from generate_invoice import InvoiceGenerator

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

TEMPLATE_DIR = Path(__file__).parent / 'templates'


def _logo_data_uri(logo_url: str, timeout: float = 12.0) -> Optional[str]:
    """Download the logo and return a base64 data URI (embedded, offline-safe)."""
    try:
        resp = requests.get(logo_url, timeout=timeout)
        resp.raise_for_status()
        ctype = resp.headers.get('Content-Type', 'image/png').split(';')[0]
        b64 = base64.b64encode(resp.content).decode('ascii')
        return f"data:{ctype};base64,{b64}"
    except Exception as e:
        logger.warning(f"⚠️  Could not fetch logo {logo_url}: {e}")
        return None


class PDFInvoiceGenerator:
    def __init__(self, parquet_file: str = 'rewards_data/rewards_master.parquet'):
        self.parquet_file = parquet_file
        self.env = Environment(
            loader=FileSystemLoader(str(TEMPLATE_DIR)),
            autoescape=select_autoescape(['html', 'xml']),
        )

    def render_pdf(self, model: dict, output_pdf: str) -> str:
        from weasyprint import HTML  # imported lazily (native libs)
        company = model['company']
        model = dict(model)
        model['logo_data_uri'] = _logo_data_uri(company.logo_url)
        html = self.env.get_template('invoice.html').render(**model)
        Path(output_pdf).parent.mkdir(parents=True, exist_ok=True)
        HTML(string=html, base_url=str(TEMPLATE_DIR)).write_pdf(output_pdf)
        logger.info(f"🧾 PDF invoice written: {output_pdf}")
        return output_pdf

    def render_xlsx(self, client_id: str, start_epoch: int, end_epoch: int,
                    output_xlsx: str, invoice_number: Optional[str] = None) -> str:
        client = get_client(client_id)
        Path(output_xlsx).parent.mkdir(parents=True, exist_ok=True)
        InvoiceGenerator(self.parquet_file).create_professional_invoice(
            output_xlsx, start_epoch, end_epoch,
            client_name=client.bill_to, invoice_number=invoice_number,
            node_ids=client.rp_node_ids,
        )
        logger.info(f"📊 XLS detail written: {output_xlsx}")
        return output_xlsx

    def generate(self, client_id: str, start_epoch: int, end_epoch: int,
                 output_pdf: Optional[str] = None, output_xlsx: Optional[str] = None,
                 eth_price_override: Optional[float] = None,
                 invoice_number: Optional[str] = None,
                 rpl_amount: Optional[float] = None):
        """Build the model once, emit both the PDF and the XLS."""
        model = build_invoice(
            client_id, start_epoch, end_epoch,
            parquet_file=self.parquet_file,
            eth_price_override=eth_price_override,
            invoice_number=invoice_number,
            rpl_amount=rpl_amount,
        )
        num = model['meta']['invoice_number']
        output_pdf = output_pdf or f"invoices/{num}.pdf"
        output_xlsx = output_xlsx or f"invoices/{num}.xlsx"

        self.render_pdf(model, output_pdf)
        self.render_xlsx(client_id, start_epoch, end_epoch, output_xlsx,
                         invoice_number=num)

        logger.info(f"✅ Invoice {num}: Balance Due ${model['balance_due_usd']:,.2f}")
        return {'pdf': output_pdf, 'xlsx': output_xlsx, 'model': model}


def main():
    ap = argparse.ArgumentParser(description='Generate a branded PDF + XLS invoice')
    ap.add_argument('client', help='Client id (see clients.json)')
    ap.add_argument('start_epoch', type=int)
    ap.add_argument('end_epoch', type=int)
    ap.add_argument('--parquet', default='rewards_data/rewards_master.parquet')
    ap.add_argument('--pdf', help='Output PDF path')
    ap.add_argument('--xlsx', help='Output XLSX path')
    ap.add_argument('--eth-price', type=float, help='Pin the ETH/USD rate')
    ap.add_argument('--rpl', type=float, help='RPL earned in the period (billable)')
    ap.add_argument('--invoice-number', help='Invoice number (e.g. INV-000067)')
    args = ap.parse_args()

    gen = PDFInvoiceGenerator(args.parquet)
    gen.generate(args.client, args.start_epoch, args.end_epoch,
                 output_pdf=args.pdf, output_xlsx=args.xlsx,
                 eth_price_override=args.eth_price,
                 invoice_number=args.invoice_number,
                 rpl_amount=args.rpl)


if __name__ == '__main__':
    main()
