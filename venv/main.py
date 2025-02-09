# main.py
import parse_protocol

def main():
    link = 'https://5verst.ru/aleksandrino/results/08.04.2023/'
    final_df_run, final_df_vol = parse_protocol.main_parse(link)
    print(final_df_run, final_df_vol)

if __name__ == '__main__':
    main()