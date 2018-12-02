package indi.zion.InfoStream.Beans;

public class Tag extends Bean{
    private int userID;
    private int movieID;
    private String tag;
    private int timeStamp;

    public Tag() {}
    
    public Tag(int userID, int movieID, String tag, int timeStamp) {
        this.userID = userID;
        this.movieID = movieID;
        this.tag = tag;
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

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public int getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Integer timeStamp) {
        this.timeStamp = timeStamp;
    }
}