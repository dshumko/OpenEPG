
OpenEPG
========

Electronic Program Guide (EPG) Generator for Digital Video Broadcasting (DVB). EIT p/f, EIT schedule generation and broadcasting.

#### Описание OpenEPG

Возможности модуля 
  * Многопоточный сервер
  * Одновременная работа с несколькими языками
  * Возможность передавать данные как в ISO, так и в Unicode
  * Передача данных по текущему транспортному потоку (Actual), так и по остальным (Other)

Передаваемые данные в таблице EIT (EPG)
  * Текущее / следующее событие
  * Расписание на несколько дней
  * Расширенное описание события (Описание, год создания, режиссер, актеры)
  * Возрастное ограничение
  * Жанр DVB


Сервер реализован на языке Perl, что позволяет его использовать на любой платформе, которую поддерживает Perl.
Базой для написания сервера послужила реализация [CherryEPG](http://epg.cherryhill.eu/|CherryEPG) и интегрирован с биллингом [A4on.TV](http://A4on.TV)

#### Установка OpenEPG

[Видео по настройке и запуску openepg](https://www.youtube.com/watch?v=Nh9wbCZjFqs) 

#### Использование бинарного файла (Windows) 
Самый простой и быстрый способ.
  - Загрузите [исполняемый файл openepg.exe](http://a4on.tv/uploads/files/openepg.zip)
  - распакуйте
  - пропишите свои данный в ini файле 
  - запустите сервер.
**Готово!**

#### Использование Perl (Windows или Linux) 

Этот способ подойдет тем, кто знаком с языком программирования Perl
Подробно данный метод описывать не будем.
Сервер использует следующие модули
```
cpan DBD::Firebird
cpan Digest::CRC
cpan DVB::Epg 
cpan DVB::Carousel
cpan Config::INI::Reader
cpan IO::Socket::Multicast
```

Как альтернатива можно поставить модули через apt-get, например:
```
apt-get install libdbd-firebird-perl 
```
Не забываем поставить make:
```
apt-get install build-essential
```
#### Описание параметров INI файла

| Параметр | Значение по умолчанию | Описание |
| --- | --- | --- |
| DB_NAME | localhost:a4on_db | база данных с epg |
| DB_USER | SYSDBA | пользователь базый данных Firebird |
| DB_PSWD | masterkey | пароль пользователя |
| DAYS    | 7 | на какое количество дней формировать EIT |
| TMP     | b:\epg.pl | где храним временные файлы |
| RELOAD_TIME | 5 | Через сколько минут перечитывать поток |
| EXPORT_TS   | 0 | Экспортировать TS в файл 1. не эксп. = 0 |
| NETWORK_ID  | 1 | ID сети с которой работает генератор на случай если у оператора много сетей и одна БД |
| BIND_IP | 192.168.1.1 | через какой сетевой интерфейс передаем UDP, например 192.168.1.1 |
| USEMEMORY | 0 | создавать файлы базы в памяти |
| ONID | '' | ONID сети с которой работает генератор |
| READ_EPG | 60 | Через сколько минут будем проверять данные в базе A4on.TV и если изменились перечитывать |
| DESC_LEN | 500 | Количество символов в описании |
| RUS_PAGE | 1 | Как кодировать язык. согласно EN 300 468, ISO/IEC 8859-5 [27] Latin/Cyrillic alphabe может быть 1 = \0x01 (Table A.3) , а может быть 2 = \0x10\0x00\0x5 (Table A.4) |
| TEXT_IN_UTF | 0 | Передавать текст событий в 1 = UTF8, 0 = ISO |
| LONGREADLEN | 1000 | Если возникает ошибка LongReadLen, снимите комментарий. 1000 можно уменьшить. |
| TOT_TDT | 0 | Формировать таблицу TOT и TDT |
| REGION_ID | 0 | Region_ID для TOT |
| PF_ONLY | 1 | Для не текущего TS создавать только таблица текущая/следующая программа present/following |

    [EPG]
    DB_NAME = localhost:D:/EPG/DB/A4on_db.fdb
    DB_USER = SYSDBA
    DB_PSWD = masterkey
    DAYS    = 7
    TMP     = b:\epg.pl
    RELOAD_TIME = 5
    EXPORT_TS   = 0
    NETWORK_ID  = 1

#### Координаты

http://A4on.TV  
twitter: a4on_tv

## License

GPL v2, see [LICENSE](LICENSE).
