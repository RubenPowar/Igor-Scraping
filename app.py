import streamlit as st

from scraper import generate_sale_data

st.set_page_config(page_title="Rightmove Scraper", layout="wide")

st.title("Rightmove Sale Data Scraper")

postcode = st.text_input("Postcode", value="NW1 7RG")

radius = st.selectbox(

    "Radius",

    options=[0.25, 0.5, 1, 3, 5, 10, 15, 20, 30, 40],

    index=2

)

start_page = st.number_input(

    "Start page",

    min_value=1,

    max_value=40,

    value=1,

    step=1

)

end_page = st.number_input(

    "End page",

    min_value=1,

    max_value=40,

    value=1,

    step=1

)

overwrite = st.checkbox("Refresh data", value=True)

if st.button("Run scraper"):

    if end_page < start_page:

        st.error("End page must be greater than or equal to start page.")

        st.stop()

    page_status = st.empty()

    page_progress = st.progress(0)

    property_status = st.empty()

    property_progress = st.progress(0)

    def update_progress(event):

        if event["stage"] == "pages":

            current_page = event["page"]

            pages_checked = event["pages_checked"]

            pages_to_check = event["pages_to_check"]

            page_progress.progress(pages_checked / pages_to_check)

            page_status.write(

                f"Checked page {current_page} "

                f"({pages_checked}/{pages_to_check} in selected range): "

                f"{event['urls_found']} new properties found "

                f"({event['total_urls']} total)."

            )

        elif event["stage"] == "properties":

            current = event["current"]

            total = event["total"]

            property_progress.progress(current / total)

            property_status.write(

                f"Scraped {event['scraped']} properties; "

                f"checked {current}/{total}. Latest status: {event['status']}."

            )

    with st.spinner("Scraping Rightmove..."):

        df = generate_sale_data(

            postcode,

            radius,

            overwrite=overwrite,

            start_page=start_page,

            end_page=end_page,

            progress_callback=update_progress

        )

    if df is None or df.empty:

        st.warning("No properties found.")

    else:

        st.success(f"Scraped {len(df)} properties.")

        st.subheader("Raw scraped data")

        st.dataframe(df.astype(str))

        csv = df.to_csv(index=False).encode("utf-8")

        st.download_button(

            "Download raw CSV",

            csv,

            file_name=f"{postcode.replace(' ', '-').upper()}_sale_prices_{radius}mi.csv",

            mime="text/csv"

        )
