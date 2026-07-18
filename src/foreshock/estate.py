"""A synthetic ML estate to exercise blast-radius analysis against.

Foreshock's claim is that a metadata change on an upstream table can be traced
to the production models it puts at risk, at the moment the change lands. Making
that claim demonstrable needs an estate with real lineage depth: raw tables feed
feature tables, feature tables feed models.

The estate is deliberately small but shaped like a real one — a column that
looks harmless to drop (``device_fingerprint``) is the input to a feature that a
fraud model depends on, so the damage is two hops away from the person making
the change and invisible in the table itself.

Everything here is emitted through DataHub's own SDK, so the resulting entities
are indistinguishable from ingested ones and produce genuine MCL traffic.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from datahub.emitter.mce_builder import (
    make_dataset_urn,
    make_ml_feature_table_urn,
    make_ml_feature_urn,
    make_ml_model_urn,
    make_schema_field_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    MLFeaturePropertiesClass,
    MLFeatureTablePropertiesClass,
    MLModelPropertiesClass,
    NumberTypeClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    TimeTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
    VersionTagClass,
)

DATA_PLATFORM = "snowflake"
MODEL_PLATFORM = "mlflow"
ENVIRONMENT = "PROD"
SEED_ACTOR = "urn:li:corpuser:datahub"

_TYPES = {
    "string": StringTypeClass,
    "number": NumberTypeClass,
    "time": TimeTypeClass,
}


@dataclass(frozen=True)
class Column:
    name: str
    kind: str
    native_type: str
    description: str = ""


@dataclass(frozen=True)
class Table:
    """One dataset in the estate, with the tables it derives from.

    ``derived_from`` maps each local column to the upstream ``(table, column)``
    it is computed from. Without it, impact analysis can only say "something
    downstream of this table" and has to warn about every model in the subtree.
    With it, a change to one column resolves to the specific columns, and so the
    specific models, that actually depend on it.
    """

    name: str
    description: str
    columns: tuple[Column, ...]
    upstreams: tuple[str, ...] = ()
    derived_from: dict[str, tuple[str, str]] = field(default_factory=dict)

    @property
    def urn(self) -> str:
        return make_dataset_urn(DATA_PLATFORM, self.name, ENVIRONMENT)


@dataclass(frozen=True)
class Feature:
    """One served feature, and the dataset column it is computed from.

    ``sources`` is what carries the dataset-to-ML edge in DataHub's model: it is
    the reason a column drop on a warehouse table is reachable from a model.
    """

    name: str
    table: str
    data_type: str
    description: str
    sources: tuple[str, ...]

    @property
    def urn(self) -> str:
        return make_ml_feature_urn(self.table, self.name)


@dataclass(frozen=True)
class FeatureTable:
    """A group of features served together."""

    name: str
    description: str

    @property
    def urn(self) -> str:
        return make_ml_feature_table_urn(MODEL_PLATFORM, self.name)


@dataclass(frozen=True)
class Model:
    """A production model and the features it consumes."""

    name: str
    description: str
    version: str
    features: tuple[str, ...] = ()
    custom_properties: dict[str, str] = field(default_factory=dict)

    @property
    def urn(self) -> str:
        return make_ml_model_urn(MODEL_PLATFORM, self.name, ENVIRONMENT)


TABLES: tuple[Table, ...] = (
    Table(
        name="raw.transactions",
        description="Raw card transaction stream, one row per authorization attempt.",
        columns=(
            Column("transaction_id", "string", "VARCHAR(36)", "Authorization id."),
            Column("customer_id", "string", "VARCHAR(36)", "Cardholder."),
            Column("amount", "number", "NUMBER(12,2)", "Authorized amount."),
            Column("currency", "string", "VARCHAR(3)", "ISO currency code."),
            Column("merchant_category", "string", "VARCHAR(8)", "MCC code."),
            Column(
                "device_fingerprint",
                "string",
                "VARCHAR(64)",
                "Hashed device signature supplied by the card-present terminal.",
            ),
            Column("occurred_at", "time", "TIMESTAMP_NTZ", "Authorization time."),
        ),
    ),
    Table(
        name="raw.customers",
        description="Customer master record.",
        columns=(
            Column("customer_id", "string", "VARCHAR(36)", "Primary key."),
            Column("signup_date", "time", "DATE", "Account open date."),
            Column("country", "string", "VARCHAR(2)", "ISO country."),
            Column("tier", "string", "VARCHAR(16)", "Pricing tier."),
            Column("email_domain", "string", "VARCHAR(128)", "Domain part only."),
        ),
    ),
    Table(
        name="features.transaction_features",
        description="Per-transaction features served to the real-time fraud model.",
        columns=(
            Column("transaction_id", "string", "VARCHAR(36)", "Join key."),
            Column("customer_id", "string", "VARCHAR(36)", "Join key."),
            Column("amount_zscore", "number", "FLOAT", "Amount vs customer mean."),
            Column("merchant_category_risk", "number", "FLOAT", "MCC risk weight."),
            Column(
                "device_fingerprint_entropy",
                "number",
                "FLOAT",
                "Shannon entropy of device_fingerprint over the trailing 30d.",
            ),
            Column("velocity_1h", "number", "NUMBER(6,0)", "Txn count, trailing hour."),
        ),
        upstreams=("raw.transactions",),
        derived_from={
            "transaction_id": ("raw.transactions", "transaction_id"),
            "customer_id": ("raw.transactions", "customer_id"),
            "amount_zscore": ("raw.transactions", "amount"),
            "merchant_category_risk": ("raw.transactions", "merchant_category"),
            "device_fingerprint_entropy": ("raw.transactions", "device_fingerprint"),
            "velocity_1h": ("raw.transactions", "transaction_id"),
        },
    ),
    Table(
        name="features.customer_features",
        description="Per-customer aggregates served to the churn model.",
        columns=(
            Column("customer_id", "string", "VARCHAR(36)", "Join key."),
            Column("tenure_days", "number", "NUMBER(6,0)", "Days since signup."),
            Column("country", "string", "VARCHAR(2)", "ISO country."),
            Column("lifetime_value", "number", "NUMBER(12,2)", "Gross margin to date."),
            Column("txn_count_30d", "number", "NUMBER(6,0)", "Txn count, trailing 30d."),
        ),
        upstreams=("raw.customers", "raw.transactions"),
        derived_from={
            "customer_id": ("raw.customers", "customer_id"),
            "tenure_days": ("raw.customers", "signup_date"),
            "country": ("raw.customers", "country"),
            # Reaches into the transaction table, so dropping `amount` does
            # endanger the churn model even though dropping
            # `device_fingerprint` does not.
            "lifetime_value": ("raw.transactions", "amount"),
            "txn_count_30d": ("raw.transactions", "transaction_id"),
        },
    ),
)

FEATURE_TABLES: tuple[FeatureTable, ...] = (
    FeatureTable(
        name="transaction_features",
        description="Online feature group for real-time authorization scoring.",
    ),
    FeatureTable(
        name="customer_features",
        description="Batch feature group for retention modelling.",
    ),
)

FEATURES: tuple[Feature, ...] = (
    Feature(
        name="amount_zscore",
        table="transaction_features",
        data_type="CONTINUOUS",
        description="Transaction amount standardised against the customer's mean.",
        sources=("features.transaction_features",),
    ),
    Feature(
        name="device_fingerprint_entropy",
        table="transaction_features",
        data_type="CONTINUOUS",
        description=(
            "Shannon entropy of the device fingerprint over 30d. Computed from "
            "raw.transactions.device_fingerprint; the strongest single signal "
            "for account-takeover fraud."
        ),
        sources=("features.transaction_features", "raw.transactions"),
    ),
    Feature(
        name="velocity_1h",
        table="transaction_features",
        data_type="COUNT",
        description="Transactions by this customer in the trailing hour.",
        sources=("features.transaction_features",),
    ),
    Feature(
        name="tenure_days",
        table="customer_features",
        data_type="COUNT",
        description="Days since the customer signed up.",
        sources=("features.customer_features",),
    ),
    Feature(
        name="lifetime_value",
        table="customer_features",
        data_type="CONTINUOUS",
        description="Gross margin contributed to date.",
        sources=("features.customer_features",),
    ),
)

MODELS: tuple[Model, ...] = (
    Model(
        name="fraud_detector",
        description=(
            "Real-time card fraud scoring, in the authorization path. "
            "A missing feature fails open and lets fraud through."
        ),
        version="3.2.1",
        features=("amount_zscore", "device_fingerprint_entropy", "velocity_1h"),
        custom_properties={
            "serving": "online",
            "criticality": "tier-1",
            "sla_ms": "40",
        },
    ),
    Model(
        name="churn_predictor",
        description="Nightly batch churn scoring for retention campaigns.",
        version="1.4.0",
        features=("tenure_days", "lifetime_value"),
        custom_properties={
            "serving": "batch",
            "criticality": "tier-3",
        },
    ),
)


def feature_by_name(name: str) -> Feature:
    return next(f for f in FEATURES if f.name == name)


def _audit_stamp() -> AuditStampClass:
    return AuditStampClass(time=0, actor=SEED_ACTOR)


def _schema_field(column: Column) -> SchemaFieldClass:
    type_class = _TYPES[column.kind]
    return SchemaFieldClass(
        fieldPath=column.name,
        type=SchemaFieldDataTypeClass(type=type_class()),
        nativeDataType=column.native_type,
        description=column.description or None,
    )


def schema_metadata(table: Table, *, drop: frozenset[str] = frozenset()) -> SchemaMetadataClass:
    """Build the schemaMetadata aspect, optionally omitting named columns.

    ``drop`` is what makes a breaking change reproducible: re-emitting a table
    without a column is exactly what a migration does, and it is what DataHub
    turns into an MCL record carrying both the old and new column sets.
    """
    columns = tuple(c for c in table.columns if c.name not in drop)
    return SchemaMetadataClass(
        schemaName=table.name,
        platform=f"urn:li:dataPlatform:{DATA_PLATFORM}",
        version=0,
        hash="",
        platformSchema=OtherSchemaClass(rawSchema=""),
        fields=[_schema_field(c) for c in columns],
        created=_audit_stamp(),
        lastModified=_audit_stamp(),
    )


def _table_proposals(
    table: Table, *, drop: frozenset[str] = frozenset()
) -> list[MetadataChangeProposalWrapper]:
    proposals = [
        MetadataChangeProposalWrapper(
            entityUrn=table.urn,
            aspect=DatasetPropertiesClass(
                name=table.name,
                description=table.description,
            ),
        ),
        MetadataChangeProposalWrapper(
            entityUrn=table.urn,
            aspect=schema_metadata(table, drop=drop),
        ),
    ]
    if table.upstreams:
        proposals.append(
            MetadataChangeProposalWrapper(
                entityUrn=table.urn,
                aspect=UpstreamLineageClass(
                    upstreams=[
                        UpstreamClass(
                            dataset=make_dataset_urn(DATA_PLATFORM, name, ENVIRONMENT),
                            type=DatasetLineageTypeClass.TRANSFORMED,
                        )
                        for name in table.upstreams
                    ],
                    fineGrainedLineages=_fine_grained(table),
                ),
            )
        )
    return proposals


def _fine_grained(table: Table) -> list[FineGrainedLineageClass]:
    """Column-to-column edges for one table, from its ``derived_from`` map."""
    edges = []
    for local_column, (upstream_table, upstream_column) in table.derived_from.items():
        upstream_urn = make_dataset_urn(DATA_PLATFORM, upstream_table, ENVIRONMENT)
        edges.append(
            FineGrainedLineageClass(
                upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                upstreams=[make_schema_field_urn(upstream_urn, upstream_column)],
                downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                downstreams=[make_schema_field_urn(table.urn, local_column)],
                confidenceScore=1.0,
            )
        )
    return edges


def _feature_proposals(feature: Feature) -> list[MetadataChangeProposalWrapper]:
    return [
        MetadataChangeProposalWrapper(
            entityUrn=feature.urn,
            aspect=MLFeaturePropertiesClass(
                description=feature.description,
                dataType=feature.data_type,
                sources=[
                    make_dataset_urn(DATA_PLATFORM, name, ENVIRONMENT)
                    for name in feature.sources
                ],
            ),
        )
    ]


def _feature_table_proposals(table: FeatureTable) -> list[MetadataChangeProposalWrapper]:
    members = [f.urn for f in FEATURES if f.table == table.name]
    return [
        MetadataChangeProposalWrapper(
            entityUrn=table.urn,
            aspect=MLFeatureTablePropertiesClass(
                description=table.description,
                mlFeatures=members,
            ),
        )
    ]


def _model_proposals(model: Model) -> list[MetadataChangeProposalWrapper]:
    return [
        MetadataChangeProposalWrapper(
            entityUrn=model.urn,
            aspect=MLModelPropertiesClass(
                name=model.name,
                description=model.description,
                version=VersionTagClass(versionTag=model.version),
                customProperties=dict(model.custom_properties),
                mlFeatures=[feature_by_name(n).urn for n in model.features],
            ),
        )
    ]


def seed_estate(emitter: DatahubRestEmitter) -> int:
    """Emit the whole estate. Returns the number of aspects written."""
    proposals: list[MetadataChangeProposalWrapper] = []
    for table in TABLES:
        proposals.extend(_table_proposals(table))
    for feature in FEATURES:
        proposals.extend(_feature_proposals(feature))
    for feature_table in FEATURE_TABLES:
        proposals.extend(_feature_table_proposals(feature_table))
    for model in MODELS:
        proposals.extend(_model_proposals(model))
    for proposal in proposals:
        emitter.emit(proposal)
    return len(proposals)


def drop_column(emitter: DatahubRestEmitter, table_name: str, column: str) -> str:
    """Re-emit one table's schema with a column removed.

    This is the change under test: it produces a real schemaMetadata MCL record
    whose ``previousAspectValue`` still holds the dropped column.
    """
    table = next(t for t in TABLES if t.name == table_name)
    emitter.emit(
        MetadataChangeProposalWrapper(
            entityUrn=table.urn,
            aspect=schema_metadata(table, drop=frozenset({column})),
        )
    )
    return table.urn


def table_by_name(name: str) -> Table:
    return next(t for t in TABLES if t.name == name)
