# main.py
import parse_protocol

def main():
    link = 'https://5verst.ru/volgogradpanorama/results/07.09.2024/'
    final_df_run, final_df_vol = parse_protocol.main_parse(link)
    print(final_df_run, final_df_vol)

if __name__ == '__main__':
    main()