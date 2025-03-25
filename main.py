import parse_protocol
import parse_table_protocols_in_park as ptpp
import parse_last_running as plr

def main():
    link = 'https://5verst.ru/aleksandrino/results/22.04.2023/'
    final_df_run, final_df_vol = parse_protocol.main_parse(link)
    print(final_df_run, final_df_vol)
    link = 'https://5verst.ru/purnavolok/results/all/'
    all_protocol = ptpp.transform_df_list_protocol(ptpp.list_protocols_in_park(link))
    print(all_protocol)
    print(plr.transform_df_last_event(plr.last_event_parse()))

if __name__ == '__main__':
    main()