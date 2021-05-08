# Pyzohar  

### Introduction
 - Pyzohar is a Python package for data manipulation.  

### Main features  
  - Basic data pre-processing between different types.  
  - Pandas.DataFrame manipulating.  
  - Data input and output between localDisc, mongodb, mysql, httpRequests, etc.  
  - Fast modelling.  

### Reference 
 - See [package Reference here](https://www.xzzsmeadow.com/#/pyzohar).  

### Examples  
Install
```cmd
> pip install pyzohar
```

Import package
```python
from pyzohar import dfz
lst_mpt = [{'A':1,'B':2},{'A':3,'B':4}]
dfz_tst = dfz(dts=lst_mpt, lcn={'fld':'./doc_smp','fls':'txt_tst.xlsx'})
dfz_tst.lcl_xpt()  # send lst_mpt into path './doc_smp/txt_tst.xlsx'
```
> Use pyzohar.dfz to output data into hard disc.  

### update 1.8  
  - Fixed bugs about misusing on pandas.Series.to_dict