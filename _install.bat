call python -m venv ..\.venv
call ..\.venv\Scripts\activate
python -m pip install --upgrade pip
pip install dbt-duckdb pandas

set p=dbt_duckdb
printf "call .venv\scripts\\activate\ncd %p%\npython -m pip install --upgrade pip\ncmd /k\n" > ..\activate_venv.bat
printf "call .venv\scripts\\activate\ncd %p%\npython -m pip install --upgrade pip\npip install -r requirements.txt --upgrade\ncmd /k\n" > ..\activate_venv_update.bat

cmd /k