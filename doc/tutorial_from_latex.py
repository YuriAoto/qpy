"""
Parses the .tex file from the documentation to generate a plain text 
version of the user tutorial

Usage: python tutorial_from_latex.py
"""

import sys
import re

def parse_line(l):
    """
    Parses the line removinf or replacing latex commands
    to something appropriate to plain text.
    """
    
    l = re.sub(r'[^\\]%.*$', '',l)

    l = l.replace(r'\qpy{}', 'qpy')
    l = l.replace(r'\qpy', 'qpy')
    l = l.replace(r'\newpage', '')


    l = l.replace(r'\begin{itemize}', '')
    l = l.replace(r'\end{itemize}', '')
    l = l.replace(r'\begin{enumerate}', '')
    l = l.replace(r'\end{enumerate}', '')
    l = l.replace(r'\item', '')

    l = l.replace(r'+$\sim$+', '~')
    l = l.replace(r'$\sim$', '~')
    l = l.replace(r'+\$+', '$')

    l = l.replace(r'\begin{lstlisting}[style=BashStyle]', '')
    l = l.replace(r'\begin{lstlisting}[style=FileStyle]', '')
    l = l.replace(r'\begin{lstlisting}', '')
    l = l.replace(r'\end{lstlisting}', '')

    l = l.replace(r'\%', '%')
    l = l.replace(r'\_', '_')
    l = l.replace(r'\#', '#')

    l = re.sub(r'\\ref\{sec:user_basics\}', 'User:Basics', l)
    l = re.sub(r'\\ref\{sec:user_commands\}', 'User:Commands', l)
    l = re.sub(r'\\ref\{sec:users\}', 'Users', l)
    l = re.sub(r'\\ref\{sec:admin_files\}', 'Administrator:Files', l)

    
    l = re.sub(r'\\label\{.+?\}', '', l)
    l = re.sub(r'\\ref\{.+?\}', '', l)

    l = re.sub(r'\\section\{(.+?)\}', r'### \1', l)
    l = re.sub(r'\\subsection\{(.+?)\}', r'## \1', l)
    l = re.sub(r'\\subsubsection\{(.+?)\}', r'# \1', l)

    l = re.sub(r'\\texttt\{(.+?)\}', r'\1', l)
    l = re.sub(r'\\emph\{(.+?)\}', r'\1', l)
    l = re.sub(r'\\verb\+(.+?)\+', r'\1', l)


    return l


try:
    f = open('common.tex','r')
except:
    sys.exit('Error when opening common.tex')

    
Allinfo = ['qpyFull',
           'qpyVersion',
           'qpyYear',
           'qpyAuthor',
           'qpyContrib']

info = {}
    
for l in f:
    for i in Allinfo:
        try:
            info[i] = re.match(r'^\\newcommand{\\' + i + '}{(.+)}', l).group(1)
        except:
            pass

f.close()

for i in Allinfo:
    if i not in info:
        sys.exit('Not all information defined in common.tex')

try:
    f = open('qpy_manual.tex','r')
except:
    sys.exit('Error when opening qpy_manual.tex')

manual = ['']
adm_manual = ['']
found_users = False
found_admin = False
for l in f:
    if '\section{For developers}' in l:
        break
    if '\section{For administrators}' in l:
        found_admin = True
    if '\section{For users}' in l:
        found_users = True
    if found_admin:
        adm_manual.append(l)
    if found_users and not(found_admin):
        manual.append(l)

f.close()

if not(found_users):
    sys.exit('Section "For users" not found in the tex file.')
if not(found_admin):
    sys.exit('Section "For administrators" not found in tex file.')

def write_header(f):
    f.write('qpy - ' + info['qpyFull'] + '\n')
    f.write(info['qpyVersion'] + ' - ' + info['qpyYear'] + '\n')
    f.write('\n')
    f.write(info['qpyAuthor'] + '\n')
    f.write(info['qpyContrib'].replace(r'{\"o}','oe') + '\n')
    f.write('\n')
    f.write('   Manual\n')
    f.write('\n')

    
try:
    f = open('tutorial','w')
except:
    sys.exit('Error when opening tutorial')

write_header(f)
for l in manual:
    f.write(parse_line(l))
f.close()

try:
    f = open('adm_tutorial','w')
except:
    sys.exit('Error when opening adm_tutorial')

write_header(f)
for l in adm_manual:
    f.write(parse_line(l))
f.close()
