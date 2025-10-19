if gen_clicked:
    if st.session_state[cart_key].empty:
        st.warning("Cart is empty.")
    else:
        # single vendor text (if multiple, stop)
        vendor_text = "_____________________________"
        if vendor_col and vendor_col in st.session_state[cart_key].columns:
            vendors = sorted(set(st.session_state[cart_key][vendor_col].dropna().astype(str).str.strip()))
            if len(vendors) == 1:
                vendor_text = vendors[0]
            elif len(vendors) > 1:
                st.error("Cart has multiple vendors. Keep only one vendor before generating.")
                st.stop()

        pncol = pn
        desc  = nm
        lines_df = pd.DataFrame({
            "Part Number": st.session_state[cart_key][pncol].astype(str) if pncol else "",
            "Description": st.session_state[cart_key][desc].astype(str)  if desc  else "",
            "Quantity":    st.session_state[cart_key]["__QTY__"].astype(str),
            "Price/Unit":  "",
            "Total":       ""
        })

        company_for_save = chosen if chosen != ADMIN_ALL else "All Companies"
        # logged-in user email from config (if present)
        user_email_cfg = (cfg.get('credentials', {})
                            .get('usernames', {})
                            .get(username, {})
                            .get('email'))
        ship_to_txt, bill_to_txt = build_ship_bill_blocks(
            ACTIVE_DB_PATH, company_for_save, user_email_cfg, name
        )

        next_no = _next_quote_number(ACTIVE_DB_PATH, datetime.utcnow())
        qid, qnum = save_quote(
            ACTIVE_DB_PATH,
            quote_number=next_no,
            company=company_for_save,
            created_by=str(username),
            vendor=vendor_text,
            ship_to=ship_to_txt,
            bill_to=bill_to_txt,
            source="restock",
            lines_df=lines_df
        )
        st.success(f"Saved Quote ID {qid} ({qnum})")

        doc_bytes = build_quote_docx(
            company=company_for_save,
            date_str=datetime.now().strftime("%Y-%m-%d"),
            quote_number=qnum,
            vendor_text=vendor_text,
            ship_to_text=ship_to_txt,
            bill_to_text=bill_to_txt,
            lines_df=lines_df
        )
        st.download_button(
            "Download Quote (Word)",
            data=doc_bytes,
            file_name=f"{qnum}_{sanitize_filename(company_for_save)}.docx",
            mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        )





