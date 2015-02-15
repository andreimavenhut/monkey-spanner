package spanner.monkey.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * User: yuyang.lan
 * Date: 2013/05/23
 * Time: 12:23
 */
public class UDFIsBot extends UDF {
    public static final String crawlerRegex = "crawler|spider|Googlebot|Mediapartners|AppEngine-Google|Yahoo!\\sSlurp|" +
            "bingbot|Prestobot|Yeti|Steeler|ichiro|hotpage\\.fr|" +
            "Feedfetcher-Google|livedoor\\sFeedFetcher|ia_archiver|YandexBot|" +
            "msnbot|zenback\\sbot|Y!J-BRO|Y!J-BRJ|Y!J-SRD|TurnitinBot|Google\\sDesktop|" +
            "BaiduMobaider|Seznam\\sscreenshot-generator|SiteBot|Purebot|emBot-GalaBuzz\\/Nutch|" +
            "Search17Bot|Tumblr|DotBot|Chilkat|Ask\\sJeeves|Ask\\.jp|Twiceler|" +
            "BlogPeople|BlogRanking|Hatena|Captain\\sAMAAN|Technoratibot|Accelatech|AppleSyndication|" +
            "Feedster|Feed::Find|Moreoverbot|wadaino";

    private Pattern pattern;


    public UDFIsBot() {
        this.pattern = Pattern.compile(crawlerRegex, Pattern.CASE_INSENSITIVE);
    }

    public boolean evaluate(String userAgentStr) {
        if (userAgentStr == null) {
            return false;
        }

        Matcher matcher = pattern.matcher(userAgentStr);
        if (matcher.find()) {
            return true;
        }

        return false;

    }
}
