@echo off
SetLocal EnableExtensions
chcp 1251

set PN=perl.exe 
set epg_dir=%cd%
rem проверим, если запущен perl.exe остановим его
TaskList /FI "ImageName EQ %PN%" 2>nul|Find /I "%PN%">nul||(taskkill /f /IM %PN%)
rem TaskList /FI "ImageName EQ %PN%" 2>nul|Find /I "%PN%"
rem If %ErrorLevel% NEQ 1 taskkill /f /IM %PN%

mode con cols=100 lines=25
title EPG ( не закрывать )

dir %epg%\tmp /a-d >nul 2>nul && (del /Q %epg%\tmp\*.sqlite)

%epg_dir%\perl\perl.exe -w %epg_dir%\OpenEPG.pl
