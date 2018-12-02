package indi.zion.InfoStream.Beans;

public class Rate extends Bean{

    private int userID;
    private int movieID;
    private double rate;
    private int timeStamp;

    public Rate() {}
    
    public Rate(int userID, int movieID, double rate, int timeStamp) {
        this.userID = userID;
        this.movieID = movieID;
        this.rate = rate;
        this.timeStamp = timeStamp;
    }

    public int getUserID() {
        return userID;
    }

    public void setUserID(Integer userID) {
        this.userID = userID;
    }

    public int getMovieID() {
        return movieID;
    }

    public void setMovieID(Integer movieID) {
        this.movieID = movieID;
    }

    public double getRate() {
        return rate;
    }

    public void setRate(Double rate) {
        this.rate = rate;
    }

    public int getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Integer timeStamp) {
        this.timeStamp = timeStamp;
    }
}