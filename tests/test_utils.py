# SPDX-FileCopyrightText: 2017-2026 CERN.
# SPDX-License-Identifier: MIT

"""Test utility functions."""

from unittest.mock import patch

import pytest

from invenio_stats.utils import get_geoip, get_user


def myfunc():
    """Example function."""
    pass


def test_get_user(app, mock_users, request_headers):
    """Test the get_user function."""
    header = request_headers["user"]
    with (
        patch("invenio_stats.utils.current_user", mock_users["authenticated"]),
        app.test_request_context(
            headers=header, environ_base={"REMOTE_ADDR": "142.0.0.1"}
        ),
    ):
        user = get_user()
    assert user["user_id"] == mock_users["authenticated"].get_id()
    assert user["user_agent"] == header["USER_AGENT"]
    assert user["ip_address"] == "142.0.0.1"


KNOWN_IP_COUNTRIES = [
    # IPv4 - Americas
    ("74.125.67.100", "US"),  # Google
    ("8.8.8.8", "US"),  # Google Public DNS
    ("128.2.42.52", "US"),  # Carnegie Mellon University
    ("128.100.3.30", "CA"),  # University of Toronto
    ("148.247.67.24", "MX"),  # Cinvestav
    ("163.178.83.117", "CR"),  # University of Costa Rica
    ("190.169.30.8", "VE"),  # Central University of Venezuela
    ("200.144.235.134", "BR"),  # University of Sao Paulo
    ("163.10.0.135", "AR"),  # National University of La Plata
    ("200.89.76.36", "CL"),  # University of Chile
    ("164.73.32.20", "UY"),  # University of the Republic
    # IPv4 - Europe
    ("188.184.76.40", "CH"),  # CERN
    ("131.169.5.106", "DE"),  # DESY
    ("194.57.3.7", "FR"),  # RENATER
    ("131.154.52.188", "IT"),  # INFN
    ("130.206.13.20", "ES"),  # RedIRIS
    ("193.137.196.247", "PT"),  # FCCN
    ("146.179.42.148", "GB"),  # Imperial College London
    ("193.1.193.205", "IE"),  # HEAnet
    ("192.16.186.153", "NL"),  # Nikhef
    ("134.58.64.12", "BE"),  # KU Leuven
    ("158.64.1.83", "LU"),  # RESTENA
    ("128.130.35.76", "AT"),  # TU Wien
    ("130.225.250.133", "DK"),  # DeiC
    ("130.237.28.40", "SE"),  # KTH Royal Institute of Technology
    ("129.240.118.130", "NO"),  # University of Oslo
    ("128.214.224.101", "FI"),  # University of Helsinki
    ("130.208.143.172", "IS"),  # University of Iceland
    ("193.40.254.225", "EE"),  # Tallinn University of Technology
    ("5.179.1.160", "LV"),  # University of Latvia
    ("158.129.0.132", "LT"),  # Kaunas University of Technology
    ("193.0.115.152", "PL"),  # University of Warsaw
    ("147.32.3.202", "CZ"),  # Czech Technical University in Prague
    ("158.195.6.138", "SK"),  # Comenius University Bratislava
    ("152.66.121.21", "HU"),  # Budapest University of Technology and Economics
    ("193.2.1.67", "SI"),  # ARNES
    ("161.53.20.179", "HR"),  # University of Zagreb
    ("147.91.1.17", "RS"),  # AMRES
    ("155.207.1.90", "GR"),  # Aristotle University of Thessaloniki
    ("62.44.96.51", "BG"),  # Sofia University
    ("141.85.220.33", "RO"),  # Politehnica University of Bucharest
    ("193.188.46.240", "MT"),  # University of Malta
    ("91.202.128.77", "UA"),  # Taras Shevchenko National University of Kyiv
    ("217.21.43.35", "BY"),  # Belarusian State University
    ("159.93.25.196", "RU"),  # JINR
    ("144.122.145.153", "TR"),  # Middle East Technical University
    # IPv4 - Middle East and Caucasus
    ("132.76.150.110", "IL"),  # Weizmann Institute of Science
    ("87.236.232.118", "JO"),  # University of Jordan
    ("212.57.212.53", "SA"),  # King Saud University
    ("178.250.249.10", "AE"),  # United Arab Emirates University
    ("194.225.72.205", "IR"),  # Institute for Research in Fundamental Sciences
    ("93.187.162.92", "AM"),  # ASNET-AM
    ("109.205.44.90", "GE"),  # Tbilisi State University
    # IPv4 - Asia and Oceania
    ("158.144.1.189", "IN"),  # Tata Institute of Fundamental Research
    ("111.68.96.165", "PK"),  # National Centre for Physics
    ("103.221.252.174", "BD"),  # University of Dhaka
    ("192.248.16.85", "LK"),  # University of Colombo
    ("162.105.131.160", "CN"),  # Peking University
    ("143.89.209.9", "HK"),  # Hong Kong University of Science and Technology
    ("140.112.8.116", "TW"),  # National Taiwan University
    ("143.248.155.65", "KR"),  # KAIST
    ("133.1.138.13", "JP"),  # Osaka University
    ("112.137.143.66", "VN"),  # Vietnam National University, Hanoi
    ("202.170.58.118", "MY"),  # Universiti Sains Malaysia
    ("152.118.147.93", "ID"),  # University of Indonesia
    ("202.90.141.7", "PH"),  # DOST Advanced Science and Technology Institute
    ("130.56.67.33", "AU"),  # Australian National University
    ("130.195.2.29", "NZ"),  # Victoria University of Wellington
    # IPv4 - Africa
    ("137.158.159.192", "ZA"),  # University of Cape Town
    ("195.246.42.89", "EG"),  # Cairo University
    ("41.89.92.134", "KE"),  # University of Nairobi
    ("196.45.48.45", "NG"),  # University of Lagos
    ("197.255.125.244", "GH"),  # University of Ghana
    ("164.160.2.81", "TN"),  # University of Tunis El Manar
    # IPv6 - Americas
    ("2001:4860:4860::8888", "US"),  # Google Public DNS
    ("2001:500:4:201::47", "US"),  # ARIN
    ("2607:f8f0:c10::100", "CA"),  # University of Victoria
    ("2001:12ff:0:4::6", "BR"),  # NIC.br
    ("2800:340:0:64::135", "AR"),  # National University of La Plata
    ("2001:1398:5::6003", "CL"),  # NIC Chile
    # IPv6 - Europe
    ("2001:1458:d00:3c::100:1ff", "CH"),  # CERN
    ("2a00:1398:b::8d03:8006", "DE"),  # Karlsruhe Institute of Technology
    ("2001:660:3001:6130::7", "FR"),  # RENATER
    ("2001:760:0:158::22", "IT"),  # GARR
    ("2001:720:418:cafd::20", "ES"),  # RedIRIS
    ("2001:690:a00:1036:1113::247", "PT"),  # FCCN
    ("2a0c:5bc0:88:100:1::172", "GB"),  # Imperial College London
    ("2001:610:508:108:192:87:108:15", "NL"),  # SURF
    ("2a02:2c40:0:80::80:12", "BE"),  # KU Leuven
    ("2001:628:101:1::135", "AT"),  # ACOnet
    ("2001:878:f500:807::6", "DK"),  # DeiC
    ("2001:6b0:1:11c2::82ed:1c28", "SE"),  # KTH Royal Institute of Technology
    ("2001:700:100:118::130", "NO"),  # University of Oslo
    ("2001:708:110:d08::65", "FI"),  # University of Helsinki
    ("2001:6d8:10:118c::6096", "PL"),  # AGH University of Krakow
    ("2001:718:ff05:108::93", "CZ"),  # CESNET
    ("2001:648:2000:329::101", "GR"),  # National Technical University of Athens
    # IPv6 - Asia and Africa
    ("2402:f000:1:402:101:6:15:66", "CN"),  # Tsinghua University
    ("2001:454e:c010:101::197", "TW"),  # TWNIC
    ("2001:218:3001:7::c0", "JP"),  # JPRS
    ("2001:df4:e000:29::212", "IN"),  # Indian Institute of Technology Delhi
    ("2001:4200:0:192a:155:232:192:35", "ZA"),  # TENET
]


@pytest.mark.parametrize("ip, country", KNOWN_IP_COUNTRIES)
def test_get_geoip(ip, country):
    """Test looking up the country of IPv4 and IPv6 addresses."""
    assert get_geoip(ip) == country
