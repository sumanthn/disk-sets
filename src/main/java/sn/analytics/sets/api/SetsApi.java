package sn.analytics.sets.api;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sn.analytics.sets.SetConfigUtils;
import sn.analytics.sets.exception.InvalidArgsException;
import sn.analytics.sets.exception.SetExistsException;
import sn.analytics.sets.exception.SetInitException;
import sn.analytics.sets.repo.BloomCache;
import sn.analytics.sets.repo.HbaseManager;
import sn.analytics.sets.repo.SetMetaDataCache;
import sn.analytics.sets.service.HbaseSetOps;
import sn.analytics.sets.service.SetDaoService;
import sn.analytics.sets.service.SetOperations;
import sn.analytics.sets.type.IdSet;
import sn.analytics.sets.type.SetMeta;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.Optional;

import static sn.analytics.sets.TypeHint.STRING;

/**
 * Created by sumanth on
 */
@Path("/v1/set")
public class SetsApi {
    private static final Logger logger = LoggerFactory.getLogger(SetsApi.class);

    @Path("{name}/create")
    @POST
    public String createSet(SetMeta setMeta, @PathParam("name") String name) throws SetInitException, SetExistsException, InvalidArgsException {

        if (setMeta == null) throw new InvalidArgsException("Set meta cannot be null");
        //name is now part of set meta cache
        // if (Strings.isNullOrEmpty(name)) throw new InvalidArgsException("Set name cannot be null");
        if (setMeta.getTypeHint() == null) {
            //set default to String
            setMeta.setTypeHint(STRING);
        }
        if (setMeta.getExpectedElements() == 0) {
            //set to a simple 10k, default expected items
            setMeta.setExpectedElements(SetConfigUtils.DEFAULT_EXPECTED_ELEMS);
        }

        //create 3 entries
        //HBase table, Bloom Filter & meta data into redis

        setMeta.setName(name);
        HbaseManager.getInstance().createTable(name);

        boolean createdCache = BloomCache.getInstance().createBloomFilter(name);
        if (createdCache) {
            //add to meta data cache
            SetMetaDataCache.getInstance().addSetCache(setMeta);
            logger.info("Created set {} {}", name, setMeta.toString());
            return "created set " + name + " " + setMeta.toString();
        }
        throw new SetInitException("Unable to create set " + name);
    }

    @Path("/{name}/add/{elem}")
    @PUT

    public boolean addElement(@PathParam("name") String name, @PathParam("elem") String element) throws Exception {
        return SetDaoService.getInstance().addElement(name, element);
    }

    @Path("/{name}/contains/{elem}")
    @GET
    //multiple options here
    //approx containment
    //force to check in true set
    //type hint for using lesser memory, fast sets, in this case use bears cost of errors
    //By default checks in bloom, its approx but for true set use param trueset = true
    //trueset may timeout ,
    //provide type hints to allow sets to be operated in client as a particular type
    //valid types include , Long, Int , Short, rest all are String
    public boolean contains(@PathParam("name") String name, @PathParam("elem") String element,
                            @QueryParam("trueset") Optional<Boolean> checkInTrueSetParam
    ) throws Exception {
        boolean checkInTrueSet = checkInTrueSetParam.orElse(false);
        if (!checkInTrueSet) {
            return SetDaoService.getInstance().checkElementApprox(name, element);
        } else {
            System.out.println("check in true set");
            return false;
        }
        //throw new IllegalArgumentException("Unknown set of options");

    }

    @Path("/{name}/elements")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public IdSet getAllElements(@PathParam("name") String name) throws Exception {

        HbaseSetOps hbaseSetOps = new HbaseSetOps();
        IdSet idSet = hbaseSetOps.getAllElements(name);
        System.out.println("id set has " + idSet.size());

        return idSet;
    }

    //union and intersection, difference
    //additional ops include union and persist a temp set

    @Path("/union/{setX}/{setY}")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public IdSet getSetOnUnion(@PathParam("setX") String setX, @PathParam("setY") String setY) throws Exception {

        if (Strings.isNullOrEmpty(setX)) throw new InvalidArgsException("Set name cannot be NULL");
        if (Strings.isNullOrEmpty(setY)) throw new InvalidArgsException("Set name cannot be NULL");

        return SetOperations.getOnUnion(setX, setY);
    }

    @Path("/union/count/{setX}/{setY}")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public long getCountOnUnion(@PathParam("setX") String setX, @PathParam("setY") String setY,
                                @QueryParam("approx") Optional<Boolean> approxParam) throws Exception {

        if (Strings.isNullOrEmpty(setX)) throw new InvalidArgsException("Set name cannot be NULL");
        if (Strings.isNullOrEmpty(setY)) throw new InvalidArgsException("Set name cannot be NULL");

        boolean approx = approxParam.orElse(false);
        return SetOperations.getCountOnUnion(setX, setY, approx);
    }


    @Path("/intersection/{setX}/{setY}")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public IdSet getSetOnIntersection(@PathParam("setX") String setX, @PathParam("setY") String setY) throws Exception {

        if (Strings.isNullOrEmpty(setX)) throw new InvalidArgsException("Set name cannot be NULL");
        if (Strings.isNullOrEmpty(setY)) throw new InvalidArgsException("Set name cannot be NULL");

        return SetOperations.getSetIntersection(setX, setY);
    }

    @Path("/intersection/count/{setX}/{setY}")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public long getCountOnIntersection(@PathParam("setX") String setX, @PathParam("setY") String setY
    ) throws Exception {

        if (Strings.isNullOrEmpty(setX)) throw new InvalidArgsException("Set name cannot be NULL");
        if (Strings.isNullOrEmpty(setY)) throw new InvalidArgsException("Set name cannot be NULL");

        return SetOperations.getSetIntersectionCount(setX, setY);
    }

    @Path("/difference/{setX}/{setY}")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public IdSet getSetOnDifference(@PathParam("setX") String setX, @PathParam("setY") String setY) throws Exception {

        if (Strings.isNullOrEmpty(setX)) throw new InvalidArgsException("Set name cannot be NULL");
        if (Strings.isNullOrEmpty(setY)) throw new InvalidArgsException("Set name cannot be NULL");

        return SetOperations.getSetDifference(setX, setY);
    }

    @Path("/difference/count/{setX}/{setY}")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public long getCountOnDifference(@PathParam("setX") String setX, @PathParam("setY") String setY) throws Exception {

        if (Strings.isNullOrEmpty(setX)) throw new InvalidArgsException("Set name cannot be NULL");
        if (Strings.isNullOrEmpty(setY)) throw new InvalidArgsException("Set name cannot be NULL");

        return SetOperations.getSetDifferenceCount(setX, setY);
    }


}
