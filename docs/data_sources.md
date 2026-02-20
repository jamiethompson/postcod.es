# Open Data Sources – MVP Street Inference Stack

This document lists the minimum required open datasets, where to obtain them,
their licence terms, and typical update frequencies.

All datasets listed below are published under the Open Government Licence v3.0 (OGL).

---

## 1. ONSUD – ONS UPRN Directory

**Purpose**  
Provides the authoritative open mapping between:
- UPRN
- Postcode (unit level)
- Statistical geographies (OA, LSOA, MSOA, LAD, etc.)

**Publisher**  
Office for National Statistics (ONS)

**Official page**  
https://geoportal.statistics.gov.uk/

Search within the portal for:
“ONS UPRN Directory”

Direct dataset landing pages may change over time; always confirm latest release from the ONS Geoportal.

**Licence**  
Open Government Licence v3.0  
https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/

**Update frequency**  
Typically quarterly.

**Notes**
- Does not contain coordinates.
- Does not contain street names.
- Forms the UPRN ↔ postcode backbone for the pipeline.

---

## 2. OS Open UPRN

**Purpose**  
Provides open coordinates for UPRNs.

**Publisher**  
Ordnance Survey

**Official dataset page**  
https://www.ordnancesurvey.co.uk/products/os-open-uprn

Open Data portal root:
https://www.ordnancesurvey.co.uk/products/os-open-data

**Licence**  
Open Government Licence v3.0  
https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/

**Update frequency**  
Typically quarterly (aligned with OS Open Data release cycle).

**Notes**
- Contains UPRN, Easting/Northing, Latitude/Longitude.
- Does not contain postcode.
- Must be joined to ONSUD via UPRN.

---

## 3. OS Open Roads

**Purpose**  
Provides road geometries and road names for spatial nearest-neighbour street inference.

**Publisher**  
Ordnance Survey

**Official dataset page**  
https://www.ordnancesurvey.co.uk/products/os-open-roads

Open Data portal root:
https://www.ordnancesurvey.co.uk/products/os-open-data

**Licence**  
Open Government Licence v3.0  
https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/

**Update frequency**  
Typically quarterly.

**Notes**
- Contains road segment geometries.
- Contains road names where available.
- Not a postal address dataset.
- Used purely for spatial inference.

---

# Summary Table

| Dataset        | Official URL | Licence | Update Frequency | Provides |
|---------------|-------------|---------|-----------------|----------|
| ONSUD | https://geoportal.statistics.gov.uk/ | OGL v3 | Quarterly | UPRN ↔ Postcode backbone |
| OS Open UPRN | https://www.ordnancesurvey.co.uk/products/os-open-uprn | OGL v3 | Quarterly | UPRN ↔ Coordinates |
| OS Open Roads | https://www.ordnancesurvey.co.uk/products/os-open-roads | OGL v3 | Quarterly | Road geometry + names |

---

# Attribution Requirement

All datasets require attribution under OGL v3.0.  
Standard attribution wording must be included in downstream documentation where required.