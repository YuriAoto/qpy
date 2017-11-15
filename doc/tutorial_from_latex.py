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
    l = l.replace(r'\item', '')

    l = l.replace(r'$\sim$', '~')

    l = l.replace(r'\begin{lstlisting}[style=BashStyle]', '')
    l = l.replace(r'\begin{lstlisting}[style=FileStyle]', '')
    l = l.replace(r'\begin{lstlisting}', '')
    l = l.replace(r'\end{lstlisting}', '')

    l = l.replace(r'\%', '%')
    l = l.replace(r'\_', '_')
    l = l.replace(r'\#', '#')

    l = re.sub(r'\\label\{.+?\}', '', l)
    l = re.sub(r'\\ref\{.+?\}', '', l)

    l = re.sub(r'\\subsection\{(.+?)\}', r'## \1', l)
    l = re.sub(r'\\subsubsection\{(.+?)\}', r'# \1', l)

    l = re.sub(r'\\texttt\{(.+?)\}', r'\1', l)
    l = re.sub(r'\\emph\{(.+?)\}', r'\1', l)


    return l



try:
    f = open('qpy_manual.tex','r')
except:
    sys.exit('Error when opening qpy_manual.tex')

manual = []
found_users = False
found_admin = False
for l in f:
    if '\section{For administrators}' in l:
        found_admin = True
        break
    if found_users:
        manual.append(l)
    if '\section{For users}' in l:
        found_users = True
f.close()

if not(found_users):
    sys.exit('Section "For users" not found in the tex file.')
if not(found_admin):
    sys.exit('Section "For administrators" not found in tex file.')

try:
    f = open('tutorial','w')
except:
    sys.exit('Error when opening tutorial')

for l in manual:
    f.write(parse_line(l))
f.close()
