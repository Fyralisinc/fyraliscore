{
  "//": "step-ca x509 leaf template for Fyralis tenant client certs (prod path).",
  "//contract": "Mirrors ca_lib.issue_tenant_cert(): clientAuth-only EKU, no CA, and the SPIFFE URI SAN spiffe://fyralis/tenant/<tenant_id>. {{ .Subject.CommonName }} / {{ .Insecure.User.tenant }} carries the tenant id supplied by the provisioner at sign time.",
  "subject": {
    "commonName": "fyralis-tenant-{{ .Insecure.User.tenant }}"
  },
  "sans": [
    {
      "type": "uri",
      "value": "spiffe://fyralis/tenant/{{ .Insecure.User.tenant }}"
    }
  ],
  "keyUsage": ["digitalSignature"],
  "extKeyUsage": ["clientAuth"],
  "basicConstraints": {
    "isCA": false
  }
}
