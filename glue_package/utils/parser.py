import argparse
from datetime import date

def cli_parser():
    ''' This function creates a parser that is used to simulate a sys.argv that takes the getResolved function in glue. 
        Kinda of a developer env for the glue getresolved option for managing dates.
        Otherwise it returns 
      '''
    if 
    # Container
    parser = argparse.ArgumentParser(description="Mi script para procesar datos")

    # deifning arguments
    parser.add_argument("--execution_date",type=date.fromisoformat,help="Fecha en formato YYYY-MM-DD",required=True)

    # 3. Read terminal
    args = parser.parse_args()

    
    return args.execution_date


