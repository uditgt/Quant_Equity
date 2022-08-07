# Quantitative Equity Research

## [Main Research Presentation](https://github.com/uditgt/quant_equity_research/blob/main/Udit%20Gupta_Summer%20Research%20(Sep%202015)_vOnline.pdf) | [Replicating Fama-French Factor model](https://github.com/uditgt/quant_equity_research/blob/main/Replicating%20Fama%20French%205%20factors.pdf)

* Developed causal linear regression model between option price movement and subsequent equity returns. Back tested and confirmed the trading strategy; demonstrated that the alpha is orthogonal to common factor models.
* Researched novel option price database (by OptionMetrics) and developed production code in SQL to integrate it with stock price database (by CRSP) and corporate-financials database (by Compustat). 
  * Developed high-fidelity logic to establish mapping between primary keys between different databases.
  * Worked with the OptionMetrics vendor to correct identified data issues at source. 
* Reproduces results from Andrew Ang's paper "The Joint Cross Section of Stocks and Options"
  * Results in Table 5 of the published paper are incorrect (no incremental predictability after Month 1). See presentation for correct results.

<p align="center"><img width="600" height="400" src='https://github.com/uditgt/quant_equity_research/blob/main/result1.png'></p>
<p align="center"><img width="600" height="400" src='https://github.com/uditgt/quant_equity_research/blob/main/result2.png'></p>
<p align="center"><img width="600" height="400" src='https://github.com/uditgt/quant_equity_research/blob/main/result3.png'></p>


* Also coded a very robust and consistent replication of monthly returns for Fama-French 5 Factor model. #DevilInTheDetails
