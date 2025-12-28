import os
import requests
import xml.etree.ElementTree as ET
import json

def run():
    bman_key = os.environ.get("BMAN_API_KEY")
    bman_url = os.environ.get("BMAN_BASE_URL")
    
    # Costruiamo il corpo SOAP esattamente come nel tuo esempio JavaScript
    soap_body = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <getAnagrafiche xmlns="http://cloud.bman.it/">
      <chiave>{bman_key}</chiave>
      <filtri><![CDATA[[]]]></filtri>
      <ordinamentoCampo>ID</ordinamentoCampo>
      <ordinamentoDirezione>1</ordinamentoDirezione>
      <numeroPagina>1</numeroPagina>
      <listaDepositi><![CDATA[]]></listaDepositi>
      <dettaglioVarianti>false</dettaglioVarianti>
    </getAnagrafiche>
  </soap:Body>
</soap:Envelope>"""

    headers = {
        'Content-Type': 'text/xml; charset=utf-8',
        'SOAPAction': 'http://cloud.bman.it/getAnagrafiche'
    }

    try:
        response = requests.post(bman_url, data=soap_body, headers=headers, timeout=15)
        
        if response.status_code == 200:
            return "✅ Connessione Bman OK! Protocollo SOAP riconosciuto correttamente."
        else:
            return f"❌ Errore Bman {response.status_code}: Verifica l'endpoint o la chiave."
            
    except Exception as e:
        raise Exception(f"❌ Errore tecnico: {str(e)}")
