from __future__ import annotations

from typing import Dict, Optional, Tuple

from fuzzywuzzy import fuzz


UNKNOWN_MARKERS = {"unknown", "no vaccines", "no vaccine", "none recorded"}


CANONICAL_VACCINES: Dict[str, Dict[str, object]] = {
    "6-in-1": {
        "program": "routine_child",
        "aliases": [
            "6-in-1", "6 in 1", "6in1", "infanrix hexa", "vaxelis",
            "dtap/ipv/hib 1", "dtap/ipv/hib 2", "dtap/ipv/hib 3",
            "infanrix-ipv+hib 1", "infanrix-ipv+hib 2", "infanrix-ipv+hib 3",
            "infanrix hexa 1", "infanrix hexa 2", "infanrix hexa 3",
            "dtap/ipv/hib/hepb", "dtap-ipv-hib-hepb",
        ],
    },
    "Rotavirus": {
        "program": "routine_child",
        "aliases": ["rotarix", "rotarix 1", "rotarix 2", "rotavirus", "rotavirus - oral 1", "rotavirus - oral 2"],
    },
    "MenB": {
        "program": "routine_child",
        "aliases": [
            "menb", "men b", "bexsero", "bexsero 1", "bexsero 2", "bexsero 3", "bexsero 4",
            "meningitis b 1", "meningitis b 2", "meningitis b 3",
        ],
    },
    "PCV": {
        "program": "routine_child",
        "aliases": [
            "pcv", "prevenar - 13 1", "prevenar - 13 2", "prevenar - 13 3",
            "prevenar 13", "pneumococcal polysaccharide conjugated vaccine (pcv) 1",
            "pneumococcal polysaccharide conjugated vaccine (pcv) 2",
            "pneumococcal polysaccharide conjugated vaccine (pcv) 3",
        ],
    },
    "Hib/MenC": {
        "program": "routine_child",
        "aliases": ["hib/menc", "menitorix", "menitorix 1st scheduled booster", "hib + meningitis c 1st scheduled booster"],
    },
    "MMR": {
        "program": "routine_child",
        "aliases": [
            "mmr", "priorix", "priorix 1", "priorix 1st scheduled booster",
            "mmrvaxpro", "mmrvaxpro 1", "mmrvaxpro 1st scheduled booster",
            "measles/mumps/rubella 1", "measles/mumps/rubella 1st scheduled booster",
            "measles/mumps/rubella under 1 yr",
        ],
    },
    "MMRV": {
        "program": "routine_child",
        "aliases": ["mmrv", "proquad", "priorix tetra", "mmr+v"],
    },
    "dTaP/IPV": {
        "program": "routine_child",
        "aliases": [
            "dtap/ipv", "repevax", "repevax booster", "repevax 1st scheduled booster",
            "boostrix-ipv", "boostrix-ipv booster", "boostrix-ipv 1st scheduled booster",
            "dtap/ipv booster", "dtap/ipv 1st scheduled booster", "dtap/ipv/hib 1st scheduled booster",
            "infanrix-ipv 1st scheduled booster", "adacel vaccine suspension for injection 0.5ml pre-filled syringes 1",
        ],
    },
    "HPV": {
        "program": "routine_child",
        "aliases": [
            "hpv", "human papillomavirus", "human papillomavirus 1", "human papillomavirus 2", "human papillomavirus 3",
            "gardasil", "gardasil 9", "gardasil9", "cervarix 1", "cervarix 2", "cervarix 3",
        ],
    },
    "Td/IPV": {
        "program": "routine_child",
        "aliases": [
            "td/ipv", "td/ipv 1", "td/ipv 2nd scheduled booster",
            "revaxis", "revaxis booster", "revaxis 2nd scheduled booster",
        ],
    },
    "MenACWY": {
        "program": "routine_child",
        "aliases": [
            "menacwy", "nimenrix", "nimenrix 1", "menquadfi",
            "meningococcal groups a, c, w and y", "meningococcal conjugate a,c, w135 + y 1",
        ],
    },
    "Flu": {
        "program": "seasonal_adult",
        "aliases": [
            "influenza vaccine 1", "flu", "fluenz tetra (astrazeneca uk ltd) 1",
            "fluenz (trivalent) vaccine nasal suspension 0.2ml unit dose (astrazeneca uk ltd) 1",
            "quadrivalent influenza vaccine split virion inactivated (sanofi pasteur) 1",
            "adjuvanted quadrivalent flu vacc (sa, inact) inj 0.5ml pfs (seqirus uk ltd) 1",
            "cell-based quadrivalent flu/vac/sa inj 0.5ml pfs (seqirus uk ltd) 1",
            "influvac sub-unit tetra (viatris formerly mylan) 1",
            "pandemrix 1",
        ],
    },
    "Pneumococcal": {
        "program": "routine_adult",
        "aliases": [
            "pneumovax 23 1", "pneumovax 23", "pneumovax23",
            "pneumococcal", "ppv23", "ppv 23",
            "pneumococcal polysaccharide vaccine (ppv) 1",
            "pneumococcal polysaccharide vaccine (ppv vial) 1",
        ],
    },
    "Shingles": {
        "program": "routine_adult",
        "aliases": ["shingles", "shingrix", "shingrix 1", "shingrix 2", "zostavax", "zostavax 1"],
    },
    "RSV": {
        "program": "routine_adult",
        "aliases": ["rsv", "abrysvo", "arexvy"],
    },
    "COVID-19": {
        "program": "history_only",
        "aliases": [
            "covid-19", "comirnaty original/omicron ba.4-5 covid-19 vacc md vials booster",
            "spikevax jn.1 covid-19 vacc 0.1mg/ml inj md vials (moderna, inc) booster",
            "comirnaty", "spikevax",
            "comirnaty omicron xbb.1.5 covid-19 vacc md vials booster",
            "comirnaty jn.1 covid-19 mrna vaccine 0.3ml inj md vials (pfizer ltd) booster",
            "covid-19 vacc spikevax (xbb.1.5) 0.1mg/1ml inj md vials booster",
        ],
    },
    "BCG": {
        "program": "history_only",
        "aliases": ["bcg", "bcg 1"],
    },
    "Hepatitis A": {
        "program": "history_only",
        "aliases": [
            "hepatitis a", "hepatitis a 1", "hepatitis a 2", "hepatitis a booster",
            "havrix monodose 2", "havrix monodose 1", "havrix mono junior monodose 1", "avaxim 1", "vaqta adult 1",
        ],
    },
    "Hepatitis B": {
        "program": "history_only",
        "aliases": ["hepatitis b", "engerix b paediatric 0.5ml booster", "engerix b 1", "engerix b 2", "engerix b 3"],
    },
    "Twinrix": {
        "program": "history_only",
        "aliases": [
            "twinrix paediatric 1", "twinrix paediatric 2", "twinrix paediatric 3",
            "combined hep a / hep b 1",
        ],
    },
    "Typhoid": {
        "program": "history_only",
        "aliases": ["typhoid 1", "typhoid booster", "typhim vi - single dose single", "typhoid single"],
    },
    "Varicella": {
        "program": "history_only",
        "aliases": ["varicella", "varivax", "varilrix"],
    },
    "Yellow Fever": {
        "program": "history_only",
        "aliases": ["yellow fever single", "yellow fever", "stamaril"],
    },
    "Rabies": {
        "program": "history_only",
        "aliases": ["rabies vaccine 1", "rabies vaccine 2", "rabies vaccine 3", "rabipur 1", "rabipur 2", "rabipur 3"],
    },
    "5-in-1": {
        "program": "history_only",
        "aliases": ["pediacel 1", "pediacel 2", "pediacel 3"],
    },
    "MenC": {
        "program": "history_only",
        "aliases": ["neisvac-c 1", "men c", "menc"],
    },
    "Hepatitis A + Typhoid": {
        "program": "history_only",
        "aliases": ["hepatitis a + typhoid 1"],
    },
    "HNIG": {
        "program": "history_only",
        "aliases": ["hnig 1"],
    },
}


def is_unknown_marker(raw_name: str) -> bool:
    return raw_name.strip().lower() in UNKNOWN_MARKERS


def normalize_vaccine_name(raw_name: str, overrides: Optional[Dict[str, Tuple[str, str]]] = None) -> Tuple[str, str, int]:
    """Map raw vaccine labels to canonical vaccine and program."""
    cleaned = " ".join(str(raw_name).strip().lower().split())
    if not cleaned:
        return "Unmapped", "history_only", 0
    if is_unknown_marker(cleaned):
        return "Unknown", "unvaccinated", 100

    if overrides and cleaned in overrides:
        canonical, program = overrides[cleaned]
        return canonical, program, 100

    for canonical, meta in CANONICAL_VACCINES.items():
        aliases = meta["aliases"]
        if cleaned == canonical.lower() or cleaned in aliases:
            return canonical, str(meta["program"]), 100

    best: Optional[Tuple[str, str, int]] = None
    for canonical, meta in CANONICAL_VACCINES.items():
        for alias in meta["aliases"]:
            score = fuzz.token_sort_ratio(cleaned, alias)
            if best is None or score > best[2]:
                best = (canonical, str(meta["program"]), score)

    if best and best[2] >= 88:
        return best

    pattern_map = [
        ("influenza", "Flu", "seasonal_adult"),
        ("fluenz", "Flu", "seasonal_adult"),
        ("pandemrix", "Flu", "seasonal_adult"),
        ("mmr", "MMR", "routine_child"),
        ("priorix tetra", "MMRV", "routine_child"),
        ("rotarix", "Rotavirus", "routine_child"),
        ("bexsero", "MenB", "routine_child"),
        ("meningitis b", "MenB", "routine_child"),
        ("prevenar", "PCV", "routine_child"),
        ("pcv", "PCV", "routine_child"),
        ("menitorix", "Hib/MenC", "routine_child"),
        ("neisvac-c", "MenC", "history_only"),
        ("repevax", "dTaP/IPV", "routine_child"),
        ("infanrix-ipv", "dTaP/IPV", "routine_child"),
        ("dtap/ipv", "dTaP/IPV", "routine_child"),
        ("cervarix", "HPV", "routine_child"),
        ("revaxis", "Td/IPV", "routine_child"),
        ("nimenrix", "MenACWY", "routine_child"),
        ("menquadfi", "MenACWY", "routine_child"),
        ("meningococcal conjugate", "MenACWY", "routine_child"),
        ("pneumovax", "Pneumococcal", "routine_adult"),
        ("pneumococcal polysaccharide", "Pneumococcal", "routine_adult"),
        ("shingrix", "Shingles", "routine_adult"),
        ("zostavax", "Shingles", "routine_adult"),
        ("abrysvo", "RSV", "routine_adult"),
        ("gardasil", "HPV", "routine_child"),
        ("comirnaty", "COVID-19", "history_only"),
        ("spikevax", "COVID-19", "history_only"),
        ("covid-19 vacc", "COVID-19", "history_only"),
        ("yellow fever", "Yellow Fever", "history_only"),
        ("rabies vaccine", "Rabies", "history_only"),
        ("pediacel", "5-in-1", "history_only"),
        ("avaxim", "Hepatitis A", "history_only"),
        ("havrix", "Hepatitis A", "history_only"),
        ("hepatitis a + typhoid", "Hepatitis A + Typhoid", "history_only"),
        ("combined hep a / hep b", "Twinrix", "history_only"),
        ("hnig", "HNIG", "history_only"),
    ]
    for token, canonical, program in pattern_map:
        if token in cleaned:
            return canonical, program, 80

    return raw_name.strip(), "history_only", 20
