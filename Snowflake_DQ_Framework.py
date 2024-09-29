import streamlit as st
from snowflake.snowpark.context import get_active_session

# Get the current session
session = get_active_session()

# Function definitions
def fetch_schemas():
    query = "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA;"
    result = session.sql(query).collect()
    return [row['SCHEMA_NAME'] for row in result]

def fetch_tables(schema_names):
    schema_list = "', '".join(schema_names)
    query = f"""
    SELECT TABLE_SCHEMA, TABLE_NAME
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA IN ('{schema_list}');
    """
    result = session.sql(query).collect()
    tables = [{'full_table_name': f"{row['TABLE_SCHEMA']}.{row['TABLE_NAME']}",
               'TABLE_SCHEMA': row['TABLE_SCHEMA'],
               'TABLE_NAME': row['TABLE_NAME']} for row in result]
    return tables

def fetch_metadata(schema_name, table_name):
    query = f"""
    SELECT COLUMN_NAME, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table_name}';
    """
    result = session.sql(query).collect()
    metadata = [{'COLUMN_NAME': row['COLUMN_NAME'], 'DATA_TYPE': row['DATA_TYPE']} for row in result]
    return metadata

def predict_column_type(column_name):
    if 'email' in column_name.lower():
        return 'email'
    elif 'phone' in column_name.lower():
        return 'phone'
    elif 'date' in column_name.lower():
        return 'date'
    else:
        return 'unknown'

def fetch_sample_data(schema_name, table_name, num_records):
    query = f"SELECT * FROM {schema_name}.{table_name} LIMIT {num_records};"
    result = session.sql(query).to_pandas()
    return result

def fetch_quality_scores(schema_name, table_name, column_name):
    completeness_query = f"""
    SELECT 
        CASE WHEN COUNT(*) = 0 THEN 0 ELSE (COUNT({column_name}) / COUNT(*)) * 100 END AS COMPLETENESS_SCORE 
    FROM {schema_name}.{table_name};
    """
    uniqueness_query = f"""
    SELECT 
        CASE WHEN COUNT({column_name}) = 0 THEN 0 ELSE (COUNT(DISTINCT {column_name}) / COUNT({column_name})) * 100 END AS UNIQUENESS_SCORE 
    FROM {schema_name}.{table_name};
    """
    completeness_score = session.sql(completeness_query).collect()[0]['COMPLETENESS_SCORE']
    uniqueness_score = session.sql(uniqueness_query).collect()[0]['UNIQUENESS_SCORE']
    return completeness_score, uniqueness_score

def apply_conformity_check(column_type, schema_name, table_name, column_name):
    if column_type == 'email':
        query = f"""
        SELECT 
            CASE WHEN {column_name} RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{{2,}}$' THEN 1 ELSE 0 END AS IS_VALID
        FROM {schema_name}.{table_name};
        """
    elif column_type == 'phone':
        query = f"""
        SELECT 
            CASE WHEN {column_name} RLIKE '^\\+?[1-9]\\d{{1,14}}$' THEN 1 ELSE 0 END AS IS_VALID
        FROM {schema_name}.{table_name};
        """
    elif column_type == 'date':
        query = f"""
        SELECT 
            CASE WHEN {column_name} RLIKE '^\\d{{4}}-\\d{{2}}-\\d{{2}}$' THEN 1 ELSE 0 END AS IS_VALID
        FROM {schema_name}.{table_name};
        """
    else:
        return 100  # Return 100% conformity for unknown types

    result = session.sql(query).collect()
    if result:
        is_valid_values = [row['IS_VALID'] for row in result]
        conformity_rate = (sum(is_valid_values) / len(is_valid_values)) * 100
    else:
        conformity_rate = 100  # No data, assume full conformity
    return conformity_rate

def generate_recommendations(completeness_score, uniqueness_score, conformity_rate):
    recommendations = []
    if completeness_score < 90:
        recommendations.append("Consider fixing null or missing values to improve completeness.")
    if uniqueness_score < 100:
        recommendations.append("Check for duplicate values to improve uniqueness.")
    if conformity_rate < 90:
        recommendations.append("Correct data entries to match the expected format and improve conformity.")
    return recommendations

# Initialize selected_tables to avoid NameError
selected_tables = []

# Streamlit app interface
st.title("📊 Snowflake Data Quality Assessment Framework")

# Sidebar for input selections
with st.sidebar:
    st.header("Select Options")
    schemas = fetch_schemas()
    selected_schemas = st.multiselect("Select Schemas", schemas)

    if selected_schemas:
        # Fetch tables for the selected schemas
        try:
            tables = fetch_tables(selected_schemas)
            table_options = [table['full_table_name'] for table in tables]
            selected_tables = st.multiselect("Select Tables", table_options)
        except Exception as e:
            st.error(f"Failed to fetch tables for the selected schemas: {e}")
            selected_tables = []

        if selected_tables:
            # Set default consent to "No"
            consent = st.radio(
                "Do you consent to sampling data for analysis? (Up to 1000 records)",
                ("Yes", "No"),
                index=1  # Default to "No"
            )

            num_records = 0
            if consent == "Yes":
                # If consent is given, show slider for sampling records
                num_records = st.slider(
                    "Select the number of records to sample (1-1000)",
                    1, 1000, 100
                )

# Ensure valid tables are selected and split schema and table names correctly
if selected_tables:
    st.header("📈 Data Maturity Summary")

    # Placeholder for analysis results
    with st.spinner("Analyzing your data..."):
        total_tables = len(selected_tables)
        total_columns = 0
        total_issues = 0

        table_summaries = {}

        for full_table_name in selected_tables:
            try:
                schema_name, table_name = full_table_name.split('.')
            except ValueError:
                st.error(f"Invalid table name format: {full_table_name}")
                continue  # Skip invalid table names

            try:
                metadata = fetch_metadata(schema_name, table_name)
            except Exception as e:
                st.error(f"Failed to fetch metadata for table {schema_name}.{table_name}: {e}")
                continue

            num_columns = len(metadata)
            total_columns += num_columns
            table_issues = []

            # Column type predictions
            if consent == "Yes" and num_records > 0:
                try:
                    sample_data = fetch_sample_data(schema_name, table_name, num_records)
                    column_types = {col: predict_column_type(col) for col in sample_data.columns}
                except Exception as e:
                    st.error(f"Failed to fetch sample data for table {schema_name}.{table_name}: {e}")
                    column_types = {}
            else:
                column_types = {row['COLUMN_NAME']: predict_column_type(row['COLUMN_NAME']) for row in metadata}

            for column_name, column_type in column_types.items():
                # Perform data quality checks
                try:
                    completeness, uniqueness = fetch_quality_scores(schema_name, table_name, column_name)
                except Exception as e:
                    st.error(f"Failed to fetch quality scores for column {column_name} in {schema_name}.{table_name}: {e}")
                    completeness, uniqueness = 100, 100

                # Perform conformity checks if applicable
                if column_type != 'unknown':
                    try:
                        conformity_rate = apply_conformity_check(column_type, schema_name, table_name, column_name)
                    except Exception as e:
                        st.error(f"Failed to perform conformity check for column {column_name} in {schema_name}.{table_name}: {e}")
                        conformity_rate = 100
                else:
                    conformity_rate = 100  # Assume full conformity for unknown types

                # Determine if there are issues
                issues = []
                if completeness < 90:
                    issues.append("Low completeness")
                if uniqueness < 100:
                    issues.append("Duplicates present")
                if conformity_rate < 90:
                    issues.append("Low conformity")

                if issues:
                    total_issues += 1
                    recommendations = generate_recommendations(completeness, uniqueness, conformity_rate)
                    table_issues.append({
                        'column_name': column_name,
                        'issues': issues,
                        'completeness': completeness,
                        'uniqueness': uniqueness,
                        'conformity': conformity_rate,
                        'recommendations': recommendations
                    })

            # Store table summary
            table_summaries[full_table_name] = {
                'issues': table_issues,
                'total_columns': num_columns
            }

        # Calculate data maturity score
        if total_columns > 0:
            data_maturity_score = ((total_columns - total_issues) / total_columns) * 100
        else:
            data_maturity_score = 100

    # Display data maturity score
    st.metric("Data Maturity Score", f"{data_maturity_score:.2f}%")

    # Provide explanation
    if data_maturity_score == 100:
        st.success("Your data has perfect quality! 🎉")
    else:
        st.warning("Some issues were detected in your data.")

    # Show reasons for the score
    st.subheader("Summary of Issues")
    st.write(f"Out of **{total_columns} columns** across **{total_tables} tables**, **{total_issues} columns** have data quality issues.")

    # Option to drill down into detailed reports
    st.subheader("Detailed Reports")

    for full_table_name, summary in table_summaries.items():
        if summary['issues']:
            with st.expander(f"Table: `{full_table_name}` - Issues Found", expanded=False):
                st.write(f"**Total Columns**: {summary['total_columns']}")
                st.write(f"**Columns with Issues**: {len(summary['issues'])}")

                for issue in summary['issues']:
                    st.write(f"---\n**Column**: `{issue['column_name']}`")
                    st.write(f"- **Issues**: {', '.join(issue['issues'])}")
                    st.write(f"- **Completeness**: {issue['completeness']:.2f}%")
                    st.write(f"- **Uniqueness**: {issue['uniqueness']:.2f}%")
                    st.write(f"- **Conformity**: {issue['conformity']:.2f}%")
                    if issue['recommendations']:
                        st.write("**Recommendations:**")
                        for rec in issue['recommendations']:
                            st.write(f"- {rec}")
        else:
            with st.expander(f"Table: `{full_table_name}` - No Issues Detected", expanded=False):
                st.write("No data quality issues detected in this table.")
else:
    st.info("Please select schemas and tables from the sidebar to begin analysis.")
