from schedule_scripts import record_latest_protocol, update_all_protocols, \
    update_recent_by_count, update_data_main, update_FIO, record_by_link, add_location_by_link


if __name__ == "__main__":
    while True:
        choice = input(
            "\nВыберите способ обновления данных:\n"
            "1 — Запустить обновление по последним протоколам\n"
            "2 — Сравнить и обновить по саммари всех протоколов\n"
            "3 — Актуализировать ФИО участников, если были изменения\n"
            "4 — Сравнить последние X протоколов\n"
            "5 — Записать/актуализировать протокол по ссылке\n"
            "6 — Добавить новую локацию по ссылке\n"
            "Ваш выбор: "
        )

        if choice == '1':
            record_latest_protocol.record_latest_protocol()
            break

        elif choice == '2':
            update_all_protocols.update_protocols()
            break

        elif choice == '3':
            update_FIO.update_FIO()
            break

        elif choice == '4':
            sub_choice = input(
                "\nВыберите режим сравнения:\n"
                "1 — Все парки\n"
                "2 — Список парков (диапазон)\n"
                "3 — Один парк\n"
                "Ваш выбор: "
            )

            if sub_choice == '1':
                try:
                    count_last_protocol = int(input("\nУкажите количество последних протоколов для сравнения (0, если все): "))
                    update_recent_by_count.find_dif_details_protocol(count_last_protocol)
                    break
                except ValueError:
                    print("Ошибка! Пожалуйста, введите целое число.")

            elif sub_choice == '2':
                park_list = update_data_main.list_point_update()
                for i, park in enumerate(park_list, 1):
                    print(f"{i}. {park}")

                try:
                    start = int(input("\nВведите начальный номер парка (от): "))
                    end = int(input("Введите конечный номер парка (до): "))

                    if not (1 <= start <= end <= len(park_list)):
                        print("Ошибка! Диапазон вне допустимых границ.")
                        continue

                    selected_parks = park_list[start - 1:end]
                    count_last_protocol = int(input("\nУкажите количество последних протоколов для сравнения (0, если все): "))

                    print(f"\nОбновление по паркам: {', '.join(selected_parks)}")
                    update_recent_by_count.find_dif_details_protocol(count_last_protocol, selected_parks)
                    break

                except (ValueError, IndexError):
                    print("Ошибка! Проверьте вводимые значения (целые числа в допустимом диапазоне).")

            elif sub_choice == '3':
                park_list = update_data_main.list_point_update()
                for i, park in enumerate(park_list, 1):
                    print(f"{i}. {park}")

                try:
                    park_choice = int(input("\nВыберите номер парка: "))
                    if not (1 <= park_choice <= len(park_list)):
                        print("Ошибка! Неверный номер парка.")
                        continue

                    name_point = park_list[park_choice - 1]
                    count_last_protocol = int(input("\nУкажите количество последних протоколов для сравнения (0, если все): "))
                    update_recent_by_count.find_dif_details_protocol(count_last_protocol, [name_point])
                    break

                except (ValueError, IndexError):
                    print("Ошибка! Проверьте правильность номера парка и количества.")

            else:
                print("Неверный ввод. Попробуйте снова.")

        elif choice == '5':
            record_by_link.record_by_link()
            break

        elif choice == '6':
            add_location_by_link.add_location_by_link()
            break

        else:
            print("Неверный ввод. Попробуйте снова.")
