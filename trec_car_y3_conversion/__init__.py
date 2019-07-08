"""trec_car_y3_conversion is a package for construction and validation of TREC CAR Y3 submission files.

Suitable for:

- constructing TREC CAR Y3 pages in the correct format (see `trec_car_y3_conversion/y3_data.py`)

- populating pages in a section-by-section fashion (see `trec_car_y3_conversion/page_population.py`)

- loading run files (see `trec_car_y3_conversion/run_file.py`)

- processing paragraph information in bulk (see `trec_car_y3_conversion/paragraph_text_collector.py`)

"""

__version__ = 1.0

__all__ = ['utils', 'run_file','y3_data', 'paragraph_text_collector', 'page_population']
