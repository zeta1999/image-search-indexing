package pt.arquivo.imagesearch.indexing.data.serializers;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import pt.arquivo.imagesearch.indexing.data.PageImageData;

import java.lang.reflect.Type;

public class PageImageDataSerializer implements JsonSerializer<PageImageData> {

    /*
    private String imgTitle;
    private String imgAlt;
    private String imgFilename;
    private String imgCaption;

    private String pageTitle;
    private String pageURLTokens;

    private String imgURL;
    private String imgURLTokens;
    private String imgSurt;

    private String pageTimestamp;
    private String pageURL;
    private String pageURLHash;

    private String pageHost;
    private String pageProtocol;

    private LocalDateTime timestamp;

    // Number of images in the original page
    private int imagesInPage;

    // Total number of matching <img src="">
    private int imgReferencesInPage;

    private boolean isInline;

    private Set<String> tagFoundIn;
     */
    @Override
    public JsonElement serialize(PageImageData src, Type typeOfSrc, JsonSerializationContext context) {
        JsonObject obj = new JsonObject();
        obj.addProperty("imgDigest", src.getImageDigest());
        obj.addProperty("type", "page");
        obj.addProperty("id", src.getId());

        obj.addProperty("imgTitle", src.getImgTitle());
        obj.addProperty("imgAlt", src.getImgAlt());
        obj.addProperty("imgFilename", src.getImgFilename());
        obj.addProperty("imgCaption", src.getImgCaption());
        obj.addProperty("imgSrc", src.getImgURL());
        obj.addProperty("imgSrcURLDigest", src.getImgURLHash());

        obj.addProperty("imgSrcTokens", src.getImgURLTokens());
        obj.addProperty("imgSurt", src.getImgSurt());

        obj.addProperty("pageTitle", src.getPageTitle());
        obj.addProperty("pageURLTokens", src.getPageURLTokens());

        obj.addProperty("imgId", src.getImgId());
        obj.addProperty("imgTstamp", src.getImgTimestamp().toString());
        obj.addProperty("imgHeight", src.getImgHeight());
        obj.addProperty("imgWidth", src.getImgWidth());
        obj.addProperty("imgMimeType", src.getImgMimeType());

        obj.addProperty("pageHost", src.getPageHost());
        obj.addProperty("pageProtocol", src.getPageProtocol());

        obj.addProperty("pageTstamp", src.getPageTimestamp().toString());
        obj.addProperty("pageURL", src.getPageURL());
        obj.addProperty("pageURLHash", src.getPageURLHash());

        obj.addProperty("imagesInOriginalPage", src.getImagesInPage());
        obj.addProperty("matchingImgReferences", src.getImgReferencesInPage());
        obj.addProperty("isInline", src.getInline());
        obj.add("tagFoundIn", context.serialize((src.getTagFoundIn())));

        obj.addProperty("warcName", src.getWarc());
        obj.addProperty("warcOffset", src.getWarcOffset());

        obj.addProperty("imgWarcName", src.getImgWarc());
        obj.addProperty("imgWarcOffset", src.getImgWarcOffset());
        obj.addProperty("collection", src.getCollection());

        obj.addProperty("safe", 0);
        obj.addProperty("spam", 0);
        obj.addProperty("blocked", 0);

        return obj;
    }
}
