"""Insère les réponses sous chaque question de l'énoncé (TP_1_de_l_ingestion_au_KPI_1.docx)."""
import copy
import re
import sys

from docx import Document
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.shared import Cm, Pt, RGBColor
from PIL import Image

SRC = "/home/user/kafka/TP_1_de_l_ingestion_au_KPI_1.docx"
DST = "/home/user/kafka/TP1_rendu/TP_1_reponses.docx"
CAP = "/home/user/kafka/TP1_rendu/captures/"
BLEU = "2E75B6"

doc = Document(SRC)
body = doc.element.body
ANCRES = [el for el in body.iterchildren()]          # index = numéro de bloc de l'énoncé

# numérotation à puces : un w:num qui pointe vers l'abstractNum à puces de l'énoncé
numbering = doc.part.numbering_part.element
abs_puce = next(a.get(qn("w:abstractNumId")) for a in numbering.findall(qn("w:abstractNum"))
                if a.find(".//" + qn("w:numFmt")).get(qn("w:val")) == "bullet")
num_ids = [int(n.get(qn("w:numId"))) for n in numbering.findall(qn("w:num"))]
NUM_PUCE = str(max(num_ids) + 1)
n = OxmlElement("w:num"); n.set(qn("w:numId"), NUM_PUCE)
a = OxmlElement("w:abstractNumId"); a.set(qn("w:val"), abs_puce); n.append(a)
numbering.append(n)


ORDRE_PPR = ["pStyle", "keepNext", "keepLines", "pageBreakBefore", "framePr", "widowControl", "numPr",
             "suppressLineNumbers", "pBdr", "shd", "tabs", "suppressAutoHyphens", "kinsoku", "wordWrap",
             "overflowPunct", "topLinePunct", "autoSpaceDE", "autoSpaceDN", "bidi", "adjustRightInd", "snapToGrid",
             "spacing", "ind", "contextualSpacing", "mirrorIndents", "suppressOverlap", "jc", "textDirection",
             "textAlignment", "textboxTightWrap", "outlineLvl", "divId", "cnfStyle", "rPr", "sectPr", "pPrChange"]
ORDRE_TBLPR = ["tblStyle", "tblpPr", "tblOverlap", "bidiVisual", "tblStyleRowBandSize", "tblStyleColBandSize", "tblW",
               "jc", "tblCellSpacing", "tblInd", "tblBorders", "shd", "tblLayout", "tblCellMar", "tblLook",
               "tblCaption", "tblDescription", "tblPrChange"]
ORDRE_TCPR = ["cnfStyle", "tcW", "gridSpan", "hMerge", "vMerge", "tcBorders", "shd", "noWrap", "tcMar", "textDirection",
              "tcFitText", "vAlign", "hideMark"]


def inserer(parent, el, ordre):
    """Insère el à sa place dans la séquence imposée par le schéma OOXML (en remplaçant un doublon éventuel)."""
    nom = el.tag.split("}")[1]
    for ancien in parent.findall(qn("w:" + nom)):
        parent.remove(ancien)
    rang = ordre.index(nom)
    for enfant in parent.iterchildren():
        n = enfant.tag.split("}")[1]
        if n in ordre and ordre.index(n) > rang:
            enfant.addprevious(el)
            return
    parent.append(el)


def filet(p, couleur=BLEU):
    """Filet vertical à gauche + retrait : marque visuellement la réponse."""
    pPr = p._p.get_or_add_pPr()
    bdr = OxmlElement("w:pBdr")
    left = OxmlElement("w:left")
    for k, v in (("val", "single"), ("sz", "12"), ("space", "8"), ("color", couleur)):
        left.set(qn("w:" + k), v)
    bdr.append(left)
    inserer(pPr, bdr, ORDRE_PPR)
    p.paragraph_format.left_indent = Cm(0.45)


def ombre(p, couleur="F2F2F2"):
    pPr = p._p.get_or_add_pPr()
    shd = OxmlElement("w:shd")
    shd.set(qn("w:val"), "clear"); shd.set(qn("w:color"), "auto"); shd.set(qn("w:fill"), couleur)
    inserer(pPr, shd, ORDRE_PPR)


def runs(p, texte, taille=None):
    """**gras** et `code` en ligne."""
    for morceau in re.split(r"(\*\*[^*]+\*\*|`[^`]+`)", texte):
        if not morceau:
            continue
        if morceau.startswith("**"):
            r = p.add_run(morceau[2:-2]); r.bold = True
        elif morceau.startswith("`"):
            r = p.add_run(morceau[1:-1]); r.font.name = "Consolas"; r.font.size = Pt(9.5)
            r.font.color.rgb = RGBColor(0x1F, 0x4E, 0x79)
        else:
            r = p.add_run(morceau)
        if taille and not morceau.startswith("`"):
            r.font.size = Pt(taille)


class Bloc:
    def __init__(self, ancre):
        self.cur = ANCRES[ancre]

    def _place(self, el):
        self.cur.addnext(el)
        self.cur = el

    def titre(self, texte="Réponse"):
        p = doc.add_paragraph()
        r = p.add_run(texte); r.bold = True; r.font.color.rgb = RGBColor(0x2E, 0x75, 0xB6); r.font.size = Pt(11)
        p.paragraph_format.space_before = Pt(6); p.paragraph_format.space_after = Pt(3)
        p.paragraph_format.keep_with_next = True
        filet(p); self._place(p._p)
        return self

    def p(self, texte):
        p = doc.add_paragraph(); runs(p, texte)
        p.paragraph_format.space_after = Pt(4)
        filet(p); self._place(p._p)
        return self

    def puces(self, items):
        for t in items:
            p = doc.add_paragraph(); runs(p, t)
            pPr = p._p.get_or_add_pPr()
            numPr = OxmlElement("w:numPr")
            il = OxmlElement("w:ilvl"); il.set(qn("w:val"), "0")
            ni = OxmlElement("w:numId"); ni.set(qn("w:val"), NUM_PUCE)
            numPr.append(il); numPr.append(ni); inserer(pPr, numPr, ORDRE_PPR)
            p.paragraph_format.space_after = Pt(2)
            filet(p)
            p.paragraph_format.left_indent = Cm(1.0); p.paragraph_format.first_line_indent = Cm(-0.45)
            self._place(p._p)
        return self

    def code(self, *lignes):
        for i, l in enumerate(lignes):
            p = doc.add_paragraph(style="Code")
            r = p.add_run(l); r.font.size = Pt(9)
            pf = p.paragraph_format
            pf.space_after = Pt(0 if i < len(lignes) - 1 else 6); pf.space_before = Pt(0)
            pf.keep_with_next = i < len(lignes) - 1
            ombre(p); filet(p); self._place(p._p)
        return self

    def img(self, nom, largeur=15.6, hauteur_max=11.0):
        w, h = Image.open(CAP + nom).size
        larg = min(largeur, hauteur_max * w / h)
        p = doc.add_paragraph()
        p.add_run().add_picture(CAP + nom, width=Cm(larg))
        p.paragraph_format.space_after = Pt(6)
        filet(p); self._place(p._p)
        return self

    def tableau(self, entetes, lignes, largeurs):
        t = doc.add_table(rows=1 + len(lignes), cols=len(entetes))
        t.style = "Table Grid"
        t.autofit = False
        tblPr = t._tbl.tblPr
        ind = OxmlElement("w:tblInd"); ind.set(qn("w:w"), str(int(0.45 / 2.54 * 1440 + 160))); ind.set(qn("w:type"), "dxa")
        inserer(tblPr, ind, ORDRE_TBLPR)
        for i, row in enumerate([entetes] + lignes):
            for j, val in enumerate(row):
                c = t.cell(i, j)
                c.width = Cm(largeurs[j])
                par = c.paragraphs[0]
                runs(par, str(val), taille=9.5)
                par.paragraph_format.space_after = Pt(0)
                par.paragraph_format.keep_with_next = i < len(lignes)
                if i == 0:
                    for r in par.runs:
                        r.bold = True
                    tcPr = c._tc.get_or_add_tcPr()
                    shd = OxmlElement("w:shd"); shd.set(qn("w:val"), "clear"); shd.set(qn("w:color"), "auto")
                    shd.set(qn("w:fill"), "DEEAF6"); inserer(tcPr, shd, ORDRE_TCPR)
        for tr in t.rows:
            trPr = tr._tr.get_or_add_trPr()
            cs = OxmlElement("w:cantSplit"); trPr.append(cs)
        self._place(t._tbl)
        espace = doc.add_paragraph(); espace.paragraph_format.space_after = Pt(2)
        self._place(espace._p)
        return self


exec(open(sys.argv[1], encoding="utf-8").read())      # contenu des réponses

doc.save(DST)
print("écrit :", DST)
