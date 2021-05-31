#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Mon May 31 10:04:01 2021
ppt generating operation.
requirements = [
  'python-pptx>=0.6.19',
]
@author: zoharslong
"""
from pyzohar.sub_slt_bsc.bsz import lsz
from pyzohar.sub_slt_bsc.dfz import dfz
from pptx import Presentation
from os.path import join as os_join, exists as os_exists
from pptx.presentation import Presentation as typ_presentation
from pptx.enum.text import MSO_ANCHOR, MSO_AUTO_SIZE, PP_ALIGN
from pptx.util import Cm, Pt, Inches


class ppz_nit(dfz):
    """
    self.lcn = {fld, fls, ppt: {
        mtr: { wdh:9144000, wtd:'16:9', },
    }}
    """
    dct_anchor = {
        'bottom': MSO_ANCHOR.BOTTOM,
        'middle': MSO_ANCHOR.MIDDLE,
        'center': MSO_ANCHOR.MIDDLE,
        'top': MSO_ANCHOR.TOP
    }  # 纵向对齐
    dct_align = {
        'justify': PP_ALIGN.JUSTIFY,  # 两端
        'left': PP_ALIGN.LEFT,
        'right': PP_ALIGN.RIGHT,
        'center': PP_ALIGN.CENTER,  # 居中
        'middle': PP_ALIGN.CENTER,  # 居中
    }   # 横向对齐
    dct_util = {
        'cm': Cm,
        'pt': Pt,
        'nch': Inches
    }    # 尺度单位
    dct_mtr = {}        # 母版的默认参数

    def __init__(self, dts=None, lcn=None, *, spr=False, cvr=True, ppt=None):
        super(ppz_nit, self).__init__(dts, lcn, spr=spr)
        self.__ppt = None
        self.ppt = ppt
        self.ppt_nit(cvr=cvr)

    @property
    def ppt(self):
        """
        self.ppt.
        :return: self.__ppt
        """
        return self.__ppt

    @ppt.setter
    def ppt(self, ppt):
        """
        set self.__ppt in self.ppt.
        :param ppt: main object from python-pptx
        :return: None
        """
        if type(ppt) in [typ_presentation, type(None)]:
            self.__ppt = ppt
        else:
            raise TypeError('info: ppt\'s type %s is not available.' % type(ppt))

    def ppt_nit(self, *, cvr=True):
        """
        pptx initiation,
        create a new empty self.ppt or get ppt from file
        :param cvr: create a new empty self.ppt to cover the old one in local file, default True
        """
        fls = os_join(self.lcn['fld'], self.lcn['fls']) if \
            os_exists(os_join(self.lcn['fld'], self.lcn['fls'])) and cvr is False else None
        self.ppt = Presentation(fls) if \
            self.ppt is None or type(self.ppt) not in [typ_presentation] or cvr is True else self.ppt

    def ppt_siz(self, wdh=9144000, wtd='16:10'):
        """
        pptx's size, width and height default [10Inches, 25.4Cm, 9144000], [6.25Inches, 15.875Cm, 5715000]
        :param wdh: width
        :param wtd: width to height, 宽高比
        """
        if 'wdh' in self.lcn['ppt']['mtr'].keys():
            self.lcn['ppt']['mtr']['wdh'] = wdh = wdh if \
                wdh is not None and self.lcn['ppt']['mtr']['wdh'] is None else self.lcn['ppt']['mtr']['wdh']
        else:
            self.lcn['ppt']['mtr']['wdh'] = wdh
        if 'wtd' in self.lcn['ppt']['mtr'].keys():
            self.lcn['ppt']['mtr']['wtd'] = wtd = wtd if \
                wtd is not None and self.lcn['ppt']['mtr']['wtd'] is None else self.lcn['ppt']['mtr']['wtd']
        else:
            self.lcn['ppt']['mtr']['wtd'] = wtd
        self.ppt.slide_width = wdh
        self.ppt.slide_height = int(wdh / float(wtd.split(':')[0]) * float(wtd.split(':')[1]))


class ppz(ppz_nit):

  def __init__(self, dts=None, lcn=None, *, spr=False, cvr=True):
    super(ppz, self).__init__(dts, lcn, spr=spr, cvr=cvr)
