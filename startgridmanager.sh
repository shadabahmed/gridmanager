# Prequisites - CAP , AST , AUTOPROCESS sections added to ast-linux.conf
# Prequisites - CAP , AUTOPROCESS for MASTER CAP and AST for MASTER AST
rm -f *settings.pyc
nohup python GridManager.py -m FULL >GridManager.log 2>>GridManager.err &