################################################################################################
#
# DAMR historical return retriever
# Created by Casey Li 2017/04/27
#
################################################################################################

################################################################################################
# Parameters
################################################################################################

#
# Load libraries
#
libs <- c("xts","hash","lubridate","readxl")
lapply(libs,function(x){library(x,character.only = TRUE)})


DAMRExtractResult <- function(request.begin.date, request.end.date){ 
  #
  # Diretory and folder manipulation
  #
  root.dir <- "S:/Asset Mix Reports/History/"
  year.map <- hash(keys=2012:2017,values=c("2012-2013",
                                           "2013-2014",
                                           "2014-2015",
                                           "2015-2016",
                                           "2016-2017",
                                           "2017"))
  
  month.before2016.map <- hash(keys=1:12,values=c("10 Jan",
                                                  "11 Feb",
                                                  "12 Mar",
                                                  "01 Apr",
                                                  "02 May",
                                                  "03 Jun",
                                                  "04 Jul",
                                                  "05 Aug",
                                                  "06 Sep",
                                                  "07 Oct",
                                                  "08 Nov",
                                                  "09 Dec"))
  
  month.after2017.map <- hash(keys=1:12,values=c("01 Jan",
                                                 "02 Feb",
                                                 "03 Mar",
                                                 "04 Apr",
                                                 "05 May",
                                                 "06 Jun",
                                                 "07 Jul",
                                                 "08 Aug",
                                                 "09 Sep",
                                                 "10 Oct",
                                                 "11 Nov",
                                                 "12 Dec"))
  damr.fn.head <- "DAMR3.0 - "
  damr.fn.tail <- ".xlsm"
  rpt.ports <- c("Analytic Canadian Equity Strategy (ACES)",
                 "Quantitative Strategic Beta",
                 "CA Low Volatility Equity",
                 "US Low Volatility Equity",
                 "International Low Volatility Equity",
                 "Emerging Market Low Volatility",
                 "Public Infrastructure",
                 "EAFE Equity (EAFEEQUP)",
                 "EAFE Equity (EAFEEQU)",
                 "US Real Estate (USREIT3)")
  
  rpt.ports.df <- data.frame(rpt.ports)
  colnames(rpt.ports.df) <- "Portfolio Name"
  
  raw.items <- c(colnames(rpt.ports.df), "Col 2", "Col 3", "Col 4", "Portfolio Return","Benchmark Return","Difference")
  rpt.items <- c(colnames(rpt.ports.df),"Portfolio Return","Benchmark Return","Difference")
  
  # Date sequence
  req.dates <- seq(request.begin.date, request.end.date, by = "day")
  rpt.core.data <- NULL
  for(i in 1:length(req.dates)){
    curr.date <- req.dates[i]
    curr.yr <- year(curr.date)
    curr.mth <- month(curr.date)
    curr.day <- day(curr.date)
    
    fldr.yr.str <- values(year.map, curr.yr)
    if(curr.yr <= 2016) {
      fldr.mth.str <- values(month.before2016.map, curr.mth)
    } else {
      fldr.mth.str <- values(month.after2017.map, curr.mth)
    }
    
    curr.fn <- paste(root.dir, fldr.yr.str, "/", fldr.mth.str, "/",
                     damr.fn.head, format(curr.date, "%Y-%m-%d"), damr.fn.tail, sep = "")
    
    #
    # Start processing files
    #
    if (file.exists(curr.fn) == TRUE) {
      #
      # Load the raw data and obtain a master table
      #
      tbl1.raw <- read_excel(curr.fn, sheet="Report", skip = 3)
      tbl1.clean <- tbl1.raw[,-c(1:5,ncol(tbl1.raw))]
      colnames(tbl1.clean) <- raw.items
      
      tbl2.raw <- read_excel(curr.fn, sheet="Report3", skip = 4)
      tbl2.clean <- tbl2.raw[,-1]
      colnames(tbl2.clean) <- raw.items
      
      master.tbl <- rbind(tbl1.clean, tbl2.clean)
      
      #
      # Retrieve the desired 
      #
      core.data.prelim <- merge(rpt.ports.df, master.tbl, by=colnames(rpt.ports.df))
      core.data.prelim <- core.data.prelim[,rpt.items]
      
      #
      # Add current date as the first column
      #
      core.data.final <- cbind(rep(curr.date, nrow(core.data.prelim)),
                               core.data.prelim)
      
      rpt.core.data <- rbind(rpt.core.data, core.data.final)
    } else {
      # Do nothing as the file doesn't exist
    }
    
  }
  colnames(rpt.core.data) <- c("Date", rpt.items)
  
  #
  # Start converting each portfolio to time seriess and merge the results
  #
  agg.data.prelim.xts <- xts(rpt.core.data[rpt.core.data[,2] %in% rpt.ports[1],-1], 
                             rpt.core.data[rpt.core.data[,2] %in% rpt.ports[1],1])
  agg.data.final.xts <- agg.data.prelim.xts[,-1]
  colnames(agg.data.final.xts) <- paste(rpt.ports[1], colnames(agg.data.final.xts))
  
  for(curr.port in rpt.ports[2:length(rpt.ports)]){
    curr.data.prelim.xts <- xts(rpt.core.data[rpt.core.data[,2] %in% curr.port,-1], 
                                rpt.core.data[rpt.core.data[,2] %in% curr.port,1])
    curr.data.final.xts <- curr.data.prelim.xts[,-1]
    colnames(curr.data.final.xts) <- paste(curr.port, colnames(curr.data.final.xts))
    
    agg.data.final.xts <- merge.xts(agg.data.final.xts,
                                    curr.data.final.xts)
  }
  is.na(agg.data.final.xts) <- 0
  cum.ret.numeric <- apply(agg.data.final.xts,2,as.numeric) 
  cum.ret.final <- Return.cumulative(cum.ret.numeric)
  
  n <- 1:3
  N <- length(rpt.ports) - 1
  cumRet.df <- NULL
  for (i in 0:N){
    cumret.temp <- cum.ret.final[,c(n+3*i)]
    cumRet.df <- rbind(cumRet.df, cumret.temp)
  }
  cumRet.df <- round(cumRet.df*10000,2)
  rownames(cumRet.df) <- rpt.ports
  colnames(cumRet.df) <- c("port (bps)","bench (bps)","active (bps)")
  
  return(list(DAMRdataDaily = agg.data.final.xts,
              DAMRdataCum = cumRet.df))
}


DAMRdata <- DAMRExtractResult(as.Date("2017-01-01"),
                              as.Date(Sys.Date()))

