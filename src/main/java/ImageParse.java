import com.sun.org.apache.xerces.internal.impl.dv.util.Base64;
import org.apache.commons.io.IOUtils;
import org.imgscalr.Scalr;
import org.imgscalr.Scalr.Method;
import org.json.simple.JSONObject;
import redis.clients.jedis.Jedis;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;


public class ImageParse {

    /**
     * @param imageURL
     * @return
     * @throws InterruptedException
     */
    public static ImageSearchResult getPropImage(String imageURL) throws InterruptedException {
        ImageSearchResult img = new ImageSearchResult();
        BufferedImage bimg;
        String base64String, base64StringOriginal;
        String type;
        URLConnection uc;
        MessageDigest digest;
        BufferedImage scaledImg;
        ByteArrayOutputStream bao = new ByteArrayOutputStream();
        try {
            int thumbWidth = 200, thumbHeight = 200;
            uc = new URL(imageURL).openConnection();
            uc.setConnectTimeout(10 * 1000);/*10 seconds*/
            //uc.setRequestMethod("GET");
            //uc.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows; U; Windows NT 6.0; en-US; rv:1.9.1.2) Gecko/20090729 Firefox/3.5.2 (.NET CLR 3.5.30729)");
            uc.connect();
            digest = MessageDigest.getInstance("SHA-256");
            InputStream inImg = uc.getInputStream();
            bimg = ImageIO.read(inImg);
            type = getMimeType(uc);
            int width = bimg.getWidth(null);
            int height = bimg.getHeight(null);
            img.setMime(type);
            img.setHeight(Double.toString(height));
            img.setWidth(Double.toString(width));
            img.setUrl(imageURL);


            // TODO image already downloaded at inImg??
            byte[] bytesImgOriginal = IOUtils.toByteArray(new URL(imageURL).openStream());
            img.setNsfw(ImageParse.getNsfwScore(Base64.encode(bytesImgOriginal)));

            //calculate digest
            digest.update(bytesImgOriginal);
            byte byteDigest[] = digest.digest();
            img.setDigest(convertByteArrayToHexString(byteDigest));

            if (width < thumbWidth || height < thumbHeight)
                scaledImg = bimg;
            else {

                if (type.equals("image/gif")) {

                    //byte[] output = getThumbnailGif( inImg , thumbWidth , thumbHeight );
                    // Create a byte array output stream.
                    if (imageURL == null)
                        return null;

                    base64StringOriginal = Base64.encode(bytesImgOriginal);
                    bao.close();
                    img.setThumbnail(base64StringOriginal);
                    return img;

                } else {
                    double thumbRatio = (double) thumbWidth / (double) thumbHeight;
                    double imageRatio = (double) width / (double) height;
                    if (thumbRatio < imageRatio)
                        thumbHeight = (int) (thumbWidth / imageRatio);
                    else
                        thumbWidth = (int) (thumbHeight * imageRatio);

                    if (width < thumbWidth && height < thumbHeight) {
                        thumbWidth = width;
                        thumbHeight = height;
                    } else if (width < thumbWidth)
                        thumbWidth = width;
                    else if (height < thumbHeight)
                        thumbHeight = height;

                    scaledImg = Scalr.resize(bimg,
                            Method.QUALITY,
                            Scalr.Mode.AUTOMATIC,
                            thumbWidth,
                            thumbHeight,
                            Scalr.OP_ANTIALIAS); //create thumbnail
                }
            }


            // Write to output stream
            ImageIO.write(scaledImg, type.substring(6), bao);
            bao.flush();

            // Create a byte array output stream.
            base64String = Base64.encode(bao.toByteArray());
            bao.close();
            img.setThumbnail(base64String);

            return img;

        } catch (MalformedURLException e) {
            e.printStackTrace();
            return null;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
            return null;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

    }


    public static BigDecimal getNsfwScore(String base64Image){
        BigDecimal score;
        Jedis jedis = new Jedis("193.136.192.183", 6379);
        String id = UUID.randomUUID().toString();

        System.out.println(id);

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("id", id);
        jsonObject.put("image", base64Image);

        jedis.rpush("image_queueing", jsonObject.toJSONString());

        // TODO use publish/subscriber keyspace notification events
        String result;
        while(true){
            result = jedis.get(id);
            if (result != null){
                break;
            }
        }

        // And if shit happens??
        if (result == null){
            score = new BigDecimal(-1);
        }
        else{
            score = new BigDecimal(result);
        }
        return score;
    }

    /**
     * get sha-256 digest string of a given string data
     *
     * @param data
     * @return
     */

    public static String hash256(String data) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        md.update(data.getBytes());
        return convertByteArrayToHexString(md.digest());
    }


    /**
     * convert the byte to hex format method (digest imgge)
     *
     * @param byteData
     * @return
     */
    private static String convertByteArrayToHexString(byte[] byteData) {
        StringBuffer hexString = new StringBuffer();
        for (int i = 0; i < byteData.length; i++) {
            String hex = Integer.toHexString(0xff & byteData[i]);
            if (hex.length() == 1) hexString.append('0');
            hexString.append(hex);
        }

        return hexString.toString();
    }

    /**
     * Get mimetype from url
     *
     * @param uc
     * @return
     * @throws java.io.IOException
     * @throws MalformedURLException
     */
    public static String getMimeType(URLConnection uc) throws java.io.IOException, MalformedURLException {
        return uc.getContentType();
    }


}