package myflink;

//用户行为类
public class UserBehavior {
    public long userId; // 用户ID

    public long itemId; // 商品ID

    public int cateId; // 类别ID

    public String behavior; // 用户行为，包括"pv点击","buy购买","cart加购","fav收藏"

    public long timestamp; // 行为发生的时间戳，秒
}
