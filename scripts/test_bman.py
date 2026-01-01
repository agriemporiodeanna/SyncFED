import os
import json
import requests
import xml.etree.ElementTree as ET


def run():
    bman_key = os.environ.get("BMAN_API_KEY")
    bman_url = os.environ.get("BMAN_BASE_URL")  # https://dominio.bman.it:3555/bmanapi.asmx

    if not bman_key or not bman_url:
        raise Exception("Variabili BMAN mancanti")

    soap_body = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xmlns:xsd="http://www.w3.org/2001/XMLSchema"
               xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <getAnagrafiche xmlns="http://cloud.bman.it/">
      <chiave>{bman_key}</chiave>
      <filtri><![CDATA[]]></filtri>
      <ordinamentoCampo>ID</ordinamentoCampo>
      <ordinamentoDirezione>1</ordinamentoDirezione>
      <numeroPagina>1</numeroPagina>
      <listaDepositi><![CDATA[[1]]]></listaDepositi>
      <dettaglioVarianti>false</dettaglioVarianti>
    </getAnagrafiche>
  </soap:Body>
</soap:Envelope>
"""

    headers = {
        "Content-Type": "text/xml; charset=utf-8",
        "SOAPAction": "http://cloud.bman.it/getAnagrafiche",
    }

    response = requests.post(
        bman_url,
        data=soap_body.encode("utf-8"),
        headers=headers,
        timeout=60,
    )

    response.raise_for_status()

    root = ET.fromstring(response.content)
    result_node = root.find(".//{http://cloud.bman.it/}getAnagraficheResult")

    if result_node is None or not result_node.text:
        raise Exception("Risposta Bman vuota")

    data = json.loads(result_node.text)

    return {
        "status": "ok",
        "message": f"Connessione Bman OK ({len(data)} articoli letti)"
    }

