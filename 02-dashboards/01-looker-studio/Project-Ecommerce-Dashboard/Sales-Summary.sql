    with
        online_retail as (
                            SELECT distinct date(InvoiceDate) as InvoiceDate,
                            Country,
                            SAFE_CAST(CustomerID as string) as CustomerID,
                            SAFE_CAST(InvoiceNo as string) as InvoiceNo,
                            SAFE_CAST(StockCode as string) as StockCode,
                            Description,
                            Quantity,
                            UnitPrice,
                            Quantity * UnitPrice as amount
                            FROM `general-364419.table_uploads.online_retail`
                            )

        select *
        from oneline_retail
        where Quantity > 0
        and FORMAT_DATE('%Y%m%d', InvoiceDate) between @DS_START_DATE and @DS_END_DATE