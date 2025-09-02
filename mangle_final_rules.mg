# MARC enrichment rules - final version

# Rules (not fact declarations)
final_title(Barcode, Title) :- google_books_data(Barcode, Title, _, _, _, _, _, _, _).
final_title(Barcode, Title) :- marc_record(Barcode, Title, _, _, _, _).

final_author(Barcode, Author) :- google_books_data(Barcode, _, Author, _, _, _, _, _, _).
final_author(Barcode, Author) :- marc_record(Barcode, _, Author, _, _, _).

final_classification(Barcode, Class) :- vertex_ai_data(Barcode, Class, _, _, _, _, _, _).
final_classification(Barcode, Class) :- google_books_data(Barcode, _, _, _, Class, _, _, _, _).

# Main enrichment rule
enriched_book(Barcode, Title, Author, Classification) :-
  final_title(Barcode, Title),
  final_author(Barcode, Author),
  final_classification(Barcode, Classification).