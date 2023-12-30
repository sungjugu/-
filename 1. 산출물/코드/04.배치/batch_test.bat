set yearmonth=%1
call mart.bat %yearmonth%
call preprocessing.bat %yearmonth%
call model.bat %yearmonth%
