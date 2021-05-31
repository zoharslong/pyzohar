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
from pptx.presentation import Presentation as typ_presentation
from os.path import join as os_join, exists as os_exists


class ppz_nit(dfz):
  """
  self.lcn = {fld, fls, ppt}
  """

  def __init__(self, dts=None, lcn=None, *, spr=False, cvr=True, ppt=None):
    super(ppz_nit, self).__init__(dts, lcn, spr=spr)
    self.__ppt = None
    self.ppt = ppt
    self.ppt_nit(cvr=cvr)

  @property
  def ppt(self):
      """
      self.location.
      :return: self.__lcn
      """
      return self.__ppt

  @ppt.setter
  def ppt(self, ppt):
      """
      set self.__lcn in self.lcn.
      :param lcn: a dict of params for self
      :return: None
      """
      if type(ppt) in [typ_presentation, type(None)]:
          self.__ppt = ppt
      else:
          raise TypeError('info: ppt\'s type %s is not available.' % type(ppt))

  def ppt_nit(self, *, cvr=True):
    if os_exists(os_join(self.lcn['fld'], self.lcn['fls'])) and cvr is False:
      fls = os_join(self.lcn['fld'], self.lcn['fls'])
    else:
      fls = None
    if self.ppt is not None and type(self.ppt) in [typ_presentation] and cvr is False:
      pass
    else:
      self.ppt = Presentation(fls)


class ppz(ppz_nit):

  def __init__(self, dts=None, lcn=None, *, spr=False, cvr=True):
    super(ppz, self).__init__(dts, lcn, spr=spr, cvr=cvr)
