@echo off
SetLocal EnableExtensions
chcp 1251

set PN=perl.exe 
set epg_dir=%cd%
rem проверим, если запущен perl.exe остановим его

rem первый вариант. иногда не работает
rem TaskList /FI "ImageName EQ %PN%" 2>nul|Find /I "%PN%">nul||(taskkill /f /IM %PN%)

rem второй вариант. иногда не работает
TaskList /FI "ImageName EQ %PN%" 2>nul|Find /I "%PN%"
If %ErrorLevel% NEQ 1 taskkill /f /IM %PN%

mode con cols=100 lines=25
title EPG ( не закрывать )

dir %epg_dir%\tmp /a-d >nul 2>nul && (del /Q %epg_dir%\tmp\*.sqlite)

%epg_dir%\perl\perl.exe -w %epg_dir%\OpenEPG.pl

