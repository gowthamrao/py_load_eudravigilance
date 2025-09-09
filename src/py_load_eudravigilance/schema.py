import sqlalchemy
from sqlalchemy.dialects.postgresql import JSONB

# Central metadata object for the entire application
metadata = sqlalchemy.MetaData()

# FRD 4.2.1: Standard Representation (Normalized Schema)
# Using sqlalchemy.TEXT for potentially long string fields to be safe.

icsr_master = sqlalchemy.Table(
    "icsr_master",
    metadata,
    sqlalchemy.Column("safetyreportid", sqlalchemy.String(255), primary_key=True),
    sqlalchemy.Column("receiptdate", sqlalchemy.String(255)),  # Version key
    sqlalchemy.Column("is_nullified", sqlalchemy.Boolean, default=False),
    sqlalchemy.Column(
        "load_timestamp", sqlalchemy.DateTime, server_default=sqlalchemy.func.now()
    ),
)

patient_characteristics = sqlalchemy.Table(
    "patient_characteristics",
    metadata,
    sqlalchemy.Column(
        "safetyreportid",
        sqlalchemy.String(255),
        sqlalchemy.ForeignKey("icsr_master.safetyreportid"),
        primary_key=True,
    ),
    sqlalchemy.Column("patientinitials", sqlalchemy.String(255)),
    sqlalchemy.Column("patientonsetage", sqlalchemy.String(255)),
    sqlalchemy.Column("patientsex", sqlalchemy.String(50)),
)

reactions = sqlalchemy.Table(
    "reactions",
    metadata,
    sqlalchemy.Column(
        "safetyreportid",
        sqlalchemy.String(255),
        sqlalchemy.ForeignKey("icsr_master.safetyreportid"),
        primary_key=True,
    ),
    sqlalchemy.Column("primarysourcereaction", sqlalchemy.TEXT),
    sqlalchemy.Column("reactionmeddrapt", sqlalchemy.TEXT, primary_key=True),
)

drugs = sqlalchemy.Table(
    "drugs",
    metadata,
    sqlalchemy.Column(
        "safetyreportid",
        sqlalchemy.String(255),
        sqlalchemy.ForeignKey("icsr_master.safetyreportid"),
        primary_key=True,
    ),
    sqlalchemy.Column("drug_seq", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("drugcharacterization", sqlalchemy.String(255)),
    sqlalchemy.Column("medicinalproduct", sqlalchemy.TEXT),
    sqlalchemy.Column("drugstructuredosagenumb", sqlalchemy.String(255)),
    sqlalchemy.Column("drugstructuredosageunit", sqlalchemy.String(255)),
    sqlalchemy.Column("drugdosagetext", sqlalchemy.TEXT),
)

# New table for drug substances, as per the plan
drug_substances = sqlalchemy.Table(
    "drug_substances",
    metadata,
    sqlalchemy.Column("safetyreportid", sqlalchemy.String(255), primary_key=True),
    sqlalchemy.Column("drug_seq", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("activesubstancename", sqlalchemy.TEXT, primary_key=True),
    sqlalchemy.ForeignKeyConstraint(
        ["safetyreportid", "drug_seq"],
        ["drugs.safetyreportid", "drugs.drug_seq"],
    ),
)


tests_procedures = sqlalchemy.Table(
    "tests_procedures",
    metadata,
    sqlalchemy.Column(
        "safetyreportid",
        sqlalchemy.String(255),
        sqlalchemy.ForeignKey("icsr_master.safetyreportid"),
        primary_key=True,
    ),
    sqlalchemy.Column("testdate", sqlalchemy.String(255)),
    sqlalchemy.Column("testname", sqlalchemy.TEXT, primary_key=True),
    sqlalchemy.Column("testresult", sqlalchemy.TEXT),
    sqlalchemy.Column("testresultunit", sqlalchemy.TEXT),
    sqlalchemy.Column("testcomments", sqlalchemy.TEXT),
)

case_summary_narrative = sqlalchemy.Table(
    "case_summary_narrative",
    metadata,
    sqlalchemy.Column(
        "safetyreportid",
        sqlalchemy.String(255),
        sqlalchemy.ForeignKey("icsr_master.safetyreportid"),
        primary_key=True,
    ),
    sqlalchemy.Column("narrative", sqlalchemy.TEXT),
)


# FRD 4.2.2: Full Representation (Audit Schema)
icsr_audit_log = sqlalchemy.Table(
    "icsr_audit_log",
    metadata,
    sqlalchemy.Column("safetyreportid", sqlalchemy.String(255), primary_key=True),
    sqlalchemy.Column("receiptdate", sqlalchemy.String(255)),  # Version key
    sqlalchemy.Column("icsr_payload", JSONB),
    sqlalchemy.Column(
        "load_timestamp",
        sqlalchemy.DateTime,
        server_default=sqlalchemy.func.now(),
    ),
)


# FRD 5.2.3: State Management / Metadata Table
etl_file_history = sqlalchemy.Table(
    "etl_file_history",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("filename", sqlalchemy.String(255), nullable=False),
    sqlalchemy.Column(
        "file_hash", sqlalchemy.String(64), nullable=False, unique=True
    ),
    sqlalchemy.Column("status", sqlalchemy.String(50), nullable=False),
    sqlalchemy.Column(
        "load_timestamp",
        sqlalchemy.DateTime,
        server_default=sqlalchemy.func.now(),
    ),
    sqlalchemy.Column("rows_processed", sqlalchemy.Integer),
)
