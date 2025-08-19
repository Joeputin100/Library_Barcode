from pymarc import Record, Field

def convert_df_to_marc(df):
    records = []
    for index, row in df.iterrows():
        record = Record()
        
        # Control Fields
        record.add_field(Field(tag='001', data=row.get('holding_barcode', '')))
        record.add_field(Field(tag='003', data='OCoLC'))
        record.add_field(Field(tag='005', data='20230101000000.0'))
        record.add_field(Field(tag='008', data='230101s2023    xxu           000 0 eng d'))

        # ISBN
        if 'isbn' in row and row['isbn']:
            record.add_field(Field(tag='020', indicators=[' ', ' '], subfields=['a', row['isbn']]))
            
        # LCCN
        if 'lccn' in row and row['lccn']:
            record.add_field(Field(tag='010', indicators=[' ', ' '], subfields=['a', row['lccn']]))

        # Title
        if 'title' in row and row['title']:
            record.add_field(Field(tag='245', indicators=['1', '0'], subfields=['a', row['title']]))

        # Author
        if 'author' in row and row['author']:
            record.add_field(Field(tag='100', indicators=['1', ' '], subfields=['a', row['author']]))

        # Publication Information
        if 'publication_date' in row and row['publication_date']:
            record.add_field(Field(tag='264', indicators=[' ', '1'], subfields=['c', row['publication_date']]))

        # Physical Description
        record.add_field(Field(tag='300', indicators=[' ', ' '], subfields=['a', '1 volume (various pagings)']))
        record.add_field(Field(tag='336', indicators=[' ', ' '], subfields=['a', 'text', 'b', 'txt', '2', 'rdacontent']))
        record.add_field(Field(tag='337', indicators=[' ', ' '], subfields=['a', 'unmediated', 'b', 'n', '2', 'rdamedia']))
        record.add_field(Field(tag='338', indicators=[' ', ' '], subfields=['a', 'volume', 'b', 'nc', '2', 'rdacarrier']))

        # Series
        if 'series_title' in row and row['series_title']:
            series_field = Field(tag='490', indicators=['1', ' '], subfields=['a', row['series_title']])
            if 'series_number' in row and row['series_number']:
                series_field.add_subfield('v', row['series_number'])
            record.add_field(series_field)

        # Summary
        if 'description' in row and row['description']:
            record.add_field(Field(tag='520', indicators=[' ', ' '], subfields=['a', row['description']]))

        # Subject Headings
        if 'subject_headings' in row and row['subject_headings']:
            subjects = row['subject_headings'].split(', ')
            for subject in subjects:
                record.add_field(Field(tag='650', indicators=[' ', '0'], subfields=['a', subject]))
        
        # Holding Information
        if 'holding_barcode' in row and row['holding_barcode']:
            holding_field = Field(tag='852', indicators=['8', ' '], subfields=['b', 's', 'h', row.get('call_number', ''), 'p', row['holding_barcode']])
            record.add_field(holding_field)

        records.append(record)
    return records

def write_marc_file(records, file_path):
    with open(file_path, 'wb') as out:
        for record in records:
            out.write(record.as_marc())