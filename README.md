# Pyzohar  

### Introduction
 - Pyzohar is a Python package for data manipulation.  

### Main features  
  - Basic data pre-processing between different types.  
  - Pandas.DataFrame manipulating.  
  - Data input and output between localDisc, mongodb, mysql, httpRequests, etc.  
  - Fast modelling.  

### Reference 
 - See [package Reference here](https://www.xzzsmeadow.com).  

### Examples  
```
from pyzohar import dfz
lst_mpt = [{'A':1,'B':2},{'A':3,'B':4}]
dfz_tst = dfz(dts=lst_mpt, lcn={'fld':'D:/','fls':'txt_tst.xlsx'})
dfz_tst.lcl_xpt()  # send lst_mpt into path 'D:/txt.tst.xlsx'
```
> Use pyzohar.dfz to output data into hard disc.  

