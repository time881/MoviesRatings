package indi.zion.InfoStream.Beans;

public class Rate{

    private int userID;
    private int movieID;
    private int rate;
    private int timeStamp;

    public Rate(int userID, int movieID, int rate, int timeStamp) {
        this.userID = userID;
        this.movieID = movieID;
        this.rate = rate;
        this.timeStamp = timeStamp;
    }

    public int getUserID() {
        return userID;
    }

    public int getMovieID() {
        return movieID;
    }

    public int getRate() {
        return rate;
    }

    public int getTimeStamp() {
        return timeStamp;
    }
}