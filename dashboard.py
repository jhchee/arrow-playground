import pyarrow as pa
import s3fs
import streamlit as st
import pyarrow.dataset as ds


@st.cache_resource
def s3_filesystem():
    return s3fs.S3FileSystem(endpoint_url="http://localhost:9000", key="admin", secret="password")


part = ds.partitioning(
    pa.schema([("date", pa.string()), ("hour", pa.int8())]),
    flavor="hive"
)

st.header("Search panel")
with (st.form("search_form")):
    _id_input = st.text_input('Input user id')
    datepicker = st.date_input('Input date', value=None)
    submitted = st.form_submit_button("Submit")
    if submitted:
        date = datepicker.strftime("%Y-%m-%d")
        dataset = ds.dataset(source=f"warehouse/parquet_output/",
                             partitioning=part,
                             format="parquet", filesystem=s3_filesystem())
        expr1 = ds.field("_id") == _id_input  # filter by _id
        expr2 = ds.field("date") == date  # filter by date
        expr = expr1 & expr2
        dataset = dataset.filter(expr)
        table = dataset.to_table()
        if len(table) == 0:
            st.write("No data found")
        else:
            st.json(table.to_pandas().to_json(orient="records", lines=True))
